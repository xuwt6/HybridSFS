# config.py
class Config:
    # Target block size: 128 MB
    TARGET_SIZE_BYTES = 128 * 1024 * 1024
    # Tolerance: 1 MB (relaxation for forcibly merging the tail block at stream end)
    TOLERANCE_BYTES = 1024 * 1024

    # HDFS configuration
    HDFS_URL = "http://192.168.31.111:9870"
    HDFS_USER = "smallfiles"
    HDFS_BASE_PATH = "/merged_small_files"

    # Elasticsearch configuration
    ES_HOST = "http://192.168.31.113:9200"
    ES_INDEX = "small_files_mapping_large_files"

    # SeaweedFS configuration (Filer HTTP interface, default port 8888)
    SEAWEDFS_FILER_URL = "http://192.168.31.111:8888"
    SEAWEDFS_BASE_PATH = "/merged_small_files"
    # Server-side chunk size (MiB): aligned with the HDFS block concept.
    # During upload, the maxMB parameter forces this value so each small file exclusively occupies 1 chunk (equivalent to 1 HDFS block),
    # making the block_count counting basis consistent with the HDFS baseline. SeaweedFS defaults maxMB to 4; here it is scaled up to 128.
    SEAWEDFS_CHUNK_SIZE_MB = 128
    # Directory listing pagination size: corresponds to the filer startup parameter -dirListLimit, default 100000
    SEAWEDFS_DIR_LIST_LIMIT = 100000


    # Genetic algorithm parameters
    GA_POPULATION_SIZE = 50       # Population size
    GA_GENERATIONS = 30           # Generations
    GA_MUTATION_RATE = 0.1        # Mutation rate
    GA_CROSSOVER_RATE = 0.8       # Crossover rate

    # Streaming simulation configuration
    LOCAL_DIR = r"E:\merge_small_files_for_memory\datatest\2-coco"
    STREAM_BATCH_SIZE = 5000      # Number of files read from disk per batch

    # Compaction configuration
    # Utilization threshold: blocks below this value are considered low-utilization blocks and need merging
    # Adjustable, but the maximum must not exceed 0.5
    COMPACTION_UTILIZATION_THRESHOLD = 0.4
    # Minimum number of low-utilization blocks required to trigger merging
    # Adjustable, but the minimum must not be less than 2
    COMPACTION_MIN_BLOCK_COUNT = 3
