#!/usr/bin/env nextflow

def helpMessage() {
    log.info """
    Usage:
    The typical command for running the pipeline is as follows:
    nextflow run main.nf --reads manifest.csv --key_file credentials.json --genome_fasta genome.fa

    --reads         Path to manifest file with csv format
    --key_file      Path to credentials file
    --gcp_id        A google cloud project ID
    --genome_fasta  Path to genome fasta file for converting CRAM files

    Check usage section from documentation for details
    """.stripIndent()
}

if (params.help) {
  helpMessage()
  exit 0
}

Channel
    .fromPath(params.reads)
    .ifEmpty { exit 1, "Cannot find CSV reads file : ${params.reads}" }
    .splitCsv(skip:1)
    .map { subj_id, file_name, md5sum, obj_id, file_size -> [subj_id, file_name, md5sum, obj_id, file_size] }
    .set { ch_gtex_gen3_ids }

Channel
    .fromPath(key_file)
    .ifEmpty { exit 1, "Key file not found: ${key_file}" }
    .set { ch_key_file }

Channel
    .fromPath(params.genome_fasta)
    .ifEmpty { exit 1, "${params.genome_fasta} is not present" }
    .set { ch_genome_fasta }

process gen3_drs_fasp {
    tag "${file_name}"
    label 'low_memory'
    
    input:
    set val(subj_id), val(file_name), val(md5sum), val(obj_id), val(file_size) from ch_gtex_gen3_ids
    each file(key_file) from ch_key_file
    each file(genome_fasta) from ch_genome_fasta
    
    output:
    set env(sample_name), file("*.bam"), val(false) into bamtofastq
    
    script:
    """
    sample_name=\$(echo ${file_name} | cut -f1 -d".")
    
    drs_url=\$(python /fasp-scripts/fasp/scripts/get_drs_url.py ${obj_id} ${params.gcp_id} ${key_file})
    signed_url=\$(echo \$drs_url | awk '\$1="";1')
    
    if [[ \$signed_url == *".bam"* ]]; then
        wget -O \${sample_name}.bam \$(echo \$signed_url)
        file_md5sum=\$(md5sum \${sample_name}.bam)
        if [[ ! "\$file_md5sum" =~ ${md5sum} ]]; then exit 1; else echo "file is good"; fi
    fi
    
    if [[ \$signed_url == *".cram"* ]]; then
        wget -O \${sample_name}.cram \$(echo \$signed_url)
        file_md5sum=\$(md5sum \${sample_name}.cram)
        if [[ ! "\$file_md5sum" =~ ${md5sum} ]]; then exit 1; else echo "file is good"; fi
        samtools view -b -T ${genome_fasta} -o \${sample_name}.bam \${sample_name}.cram
    fi
    """
  }