python evaluate_variants/vqa_evaluate_orig.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model wav2vec2-base-960h
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model wav2vec2-large-960h-lv60-self
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model wav2vec2-large-robust-ft-libri-960h
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model s2t-small-librispeech-asr
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model s2t-medium-librispeech-asr
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model s2t-large-librispeech-asr
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model hubert-large-ls960-ft
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model hubert-xlarge-ls960-ft
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model sew-d-tiny-100k-ft-ls100h
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model sew-d-mid-400k-ft-ls100h
python evaluate_variants/vqa_evaluate_asr.py --path_opt options/variants/mutan_noatt_resnet101.yaml -b 512 -j 4 --asr_model sew-d-base-plus-400k-ft-ls100h