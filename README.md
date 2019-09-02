## fastqQC

Data preprpcessing pipeline of raw sequencing data.

## Author

Dechun Lin

## Requirements

* A UNIX based operating system.
* python 3
* python pacakges: pandas
* [fastp](https://github.com/OpenGene/fastp)
* SOAPnuke

## Installation
Download fastqQC from GitHub.

```shell
git clone https://github.com/lindechun/fastqQC
python3 /your/path/to/fastqQC/bin/fastqQC.py -h
```

After installed, you need link fastp and SOAPnuke to `/your/path/to/fastqQC/src/` or install to `/your/path/to/fastqQC/src`

## demo
### pe
`python3 /your/path/to/fastqQC/bin/fastqQC.py -i pe_raw.list -o result_pe -s pe -t 10 -p test_pe --mRun --runNumber 4`
### se
`python3 /your/path/to/fastqQC/bin/fastqQC.py -i se_raw.list -o result_se -s se -t 10 -p test_se --mRun --runNumber 4`
