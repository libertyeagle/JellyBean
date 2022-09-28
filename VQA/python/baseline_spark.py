import __main__

import os
import time
from os import environ, listdir, path
import json
import torch
from pyspark import SparkFiles, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

from feature_extractor import ImageFeatureExtractor
from speech_recognition import SpeechRecognitionEngine
from vqa_inference import VQAInferenceEngine
from utils import read_audio_files, load_images

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class Log4j(object):
    """Wrapper class for Log4j JVM object.
    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log a warning.
        :param: Warning message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in 'config.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()
    
    conf = SparkConf()
    if "PYTHONPATH" in spark_config:
        conf = conf.setExecutorEnv("PYTHONPATH", spark_config["PYTHONPATH"])
    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .config(conf=conf)
            .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('.json') and ('config' in filename or 'setting' in filename)]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict


def map_dataset(dataset, feature_extractor, asr_engine, vqa_engine):
    results = []
    for row in dataset:
        print("Processing Image-Question Pair #{}".format(row["id"]))
        audio = row["audio"]
        image = row["image"]
        question = asr_engine.transcribe_audio(audio)
        image_feat = feature_extractor.extract_features(image)
        answer = vqa_engine.generate_answer(image_feat, question)
        results.append((row['id'], answer))
    torch.cuda.synchronize()
    return iter(results)


def read_dataset(dataset_path, num_instances):
    question_json_path = os.path.join(
        dataset_path, "MultipleChoice_mscoco_val2014_questions.json")
    questions = json.load(open(question_json_path, 'rt'))["questions"]

    image_dir = os.path.join(dataset_path, "coco_images/val2014")
    audio_dir = os.path.join(dataset_path, "questions_speech/val2014")

    image_paths = []
    audio_paths = []
    for question in questions:
        image_id = question["image_id"]
        question_id = question["question_id"]
        image_path = os.path.join(
            image_dir, "COCO_val2014_{:012d}.jpg".format(image_id))
        audio_path = os.path.join(audio_dir, "{}.flac".format(question_id))
        image_paths.append((question_id, image_path))
        audio_paths.append((question_id, audio_path))

    image_paths.sort(key=lambda x: x[0])
    audio_paths.sort(key=lambda x: x[0])

    image_paths = image_paths[:num_instances]
    audio_paths = audio_paths[:num_instances]
    image_paths = list(map(lambda x: (x[0], x[1][1]), enumerate(image_paths)))
    audio_paths = list(map(lambda x: (x[0], x[1][1]), enumerate(audio_paths)))

    return image_paths, audio_paths


def main():
    spark_config = { "PYTHONPATH": DIR_PATH }
    spark, log, config = start_spark(
        app_name='vqa_job',
        files=[],
        spark_config=spark_config)

    if config is None:
        raise ValueError("No config file provided.")

    device = "cuda:0" if torch.cuda.is_available() else 'cpu'
    feature_extractor_config = {
        "model": config["image_model"],
        "device": device,
        "img_size": 448,
        "hub_dir": None,
        "use_att_feat": False
    }

    asr_config = {
        "model": config["asr_model"],
        "device": device,
        "sampling_rate": 16000,
        "cache_dir": None
    }

    image_model_choice = config["image_model"]
    if image_model_choice == "resnet18":
        yaml_path = os.path.join(
            DIR_PATH, "configs/variants/mutan_noatt_resnet18.yaml")
        ckpt_path = os.path.join(
            DIR_PATH, "trained_models/variants/mutan_noatt_resnet18")
    elif image_model_choice == "resnet34":
        yaml_path = os.path.join(
            DIR_PATH, "configs/variants/mutan_noatt_resnet34.yaml")
        ckpt_path = os.path.join(
            DIR_PATH, "trained_models/variants/mutan_noatt_resnet34")
    elif image_model_choice == "resnet50":
        yaml_path = os.path.join(
            DIR_PATH, "configs/variants/mutan_noatt_resnet50.yaml")
        ckpt_path = os.path.join(
            DIR_PATH, "trained_models/variants/mutan_noatt_resnet50")
    elif image_model_choice == "resnet101":
        yaml_path = os.path.join(
            DIR_PATH, "configs/variants/mutan_noatt_resnet101.yaml")
        ckpt_path = os.path.join(
            DIR_PATH, "trained_models/variants/mutan_noatt_resnet101")
    elif image_model_choice == "resnet152":
        yaml_path = os.path.join(
            DIR_PATH, "configs/variants/mutan_noatt_resnet152.yaml")
        ckpt_path = os.path.join(
            DIR_PATH, "trained_models/variants/mutan_noatt_resnet152")
    else:
        raise ValueError("invalid image model")
    vqa_config = {
        "config_path": yaml_path,
        "ckpt_path": ckpt_path,
        "resume_ckpt": "ckpt",
        "device": device
    }

    dataset_dir = os.path.expanduser(config["dataset_path"])
    num_instances = config.get("num_instances", 1024)
    num_partitions = config["num_spark_partitions"]
    image_paths, audio_paths = read_dataset(
        dataset_dir, num_instances=num_instances)
    image_paths = [x[1] for x in image_paths]
    audio_paths = [x[1] for x in audio_paths]

    images = [] 
    audios = []
    for image_path, audio_path in zip(image_paths, audio_paths):
        audios.append(read_audio_files(audio_path, sampling_rate=16000))
        images.append(load_images(image_path))

    data = []
    for id, (image, audio) in enumerate(zip(images, audios)):
        data.append(Row(id=id, image=image, audio=audio))

    feature_extractor = ImageFeatureExtractor(feature_extractor_config)
    asr_engine = SpeechRecognitionEngine(asr_config)
    vqa_engine = VQAInferenceEngine(vqa_config)

    start = time.time()

    rdd = spark.sparkContext.parallelize(data, numSlices=num_partitions)
    results = rdd.mapPartitions(lambda ds: map_dataset(ds, feature_extractor, asr_engine, vqa_engine))
    results.collectAsMap()
    del results

    rdd = spark.sparkContext.parallelize(data, numSlices=num_partitions)
    results = rdd.mapPartitions(lambda ds: map_dataset(ds, feature_extractor, asr_engine, vqa_engine))
    results.collectAsMap()
    del results

    rdd = spark.sparkContext.parallelize(data, numSlices=num_partitions)
    results = rdd.mapPartitions(lambda ds: map_dataset(ds, feature_extractor, asr_engine, vqa_engine))
    results.collectAsMap()

    end = time.time()
    runtime = end - start
    log.info("Total runtime={:.2f}s".format(runtime))
    log.info("Throughput={:.4f}".format(num_instances * 3 / runtime))


if __name__ == "__main__":
    main()
