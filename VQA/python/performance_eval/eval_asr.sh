mkdir -p ../perf_measure
truncate -s 0 ../perf_measure/asr_model.txt

python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch wav2vec2-base-960h
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch wav2vec2-large-960h-lv60-self
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch wav2vec2-large-robust-ft-libri-960h
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch s2t-small-librispeech-asr
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch s2t-medium-librispeech-asr
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch s2t-large-librispeech-asr
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch hubert-large-ls960-ft
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch hubert-xlarge-ls960-ft
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch sew-d-tiny-100k-ft-ls100h
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch sew-d-mid-400k-ft-ls100h
python performance_eval/performance_eval_asr.py -b 8 -d 0 --log_file ../perf_measure/asr_model.txt --arch sew-d-base-plus-400k-ft-ls100h