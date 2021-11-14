import sys
import os
import time
from pdf2image import convert_from_path

def dummygenerator(name: str):
    yield name

if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_folder = sys.argv[2]

    raw_files = [f for f in os.listdir(input_dir) if f.endswith('.pdf')]
    nr_files = len(raw_files)

    start = time.time()
    nr_pages = 0
    for i, pdf in enumerate(raw_files):
        input_file = os.path.join(input_dir, pdf)
        output_file = pdf[:-4]
        print(f'Input: {input_file}')
        print(f'Output file: {output_file}')
        result = convert_from_path(
            input_file,
            output_folder=output_folder,
            fmt='png',
            output_file=dummygenerator(output_file)
        )
        nr_pages += len(result)
        print(f'Rasterized {i+1}/{nr_files} PDFs.')
    end = time.time()

    elapsed_time = end - start

    print(f'Rasterized {nr_pages} pages in {elapsed_time/60} minutes, at an avg. of {elapsed_time/nr_pages}s per page.')
    
    
    