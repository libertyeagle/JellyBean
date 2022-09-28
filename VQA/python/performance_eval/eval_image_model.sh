mkdir -p ../perf_measure
truncate -s 0 ../perf_measure/image_model.txt

python performance_eval/performance_eval_image_model.py -b 8 -d 0 --log_file ../perf_measure/image_model.txt --arch resnet18
python performance_eval/performance_eval_image_model.py -b 8 -d 0 --log_file ../perf_measure/image_model.txt --arch resnet34
python performance_eval/performance_eval_image_model.py -b 8 -d 0 --log_file ../perf_measure/image_model.txt --arch resnet50
python performance_eval/performance_eval_image_model.py -b 8 -d 0 --log_file ../perf_measure/image_model.txt --arch resnet101
python performance_eval/performance_eval_image_model.py -b 8 -d 0 --log_file ../perf_measure/image_model.txt --arch resnet152