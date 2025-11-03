class SparkConfig:
    """Spark configuration for different environments"""
    
    LOCAL_CONFIG = {
        "master": "local[*]",
        "app_name": "LocalDataExtractor"
    }
    
    CLUSTER_CONFIG = {
        "master": "spark://your-cluster-master:7077",
        "app_name": "ClusterDataExtractor",
        "configs": {
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.executor.instances": "3",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    }
    
    YARN_CONFIG = {
        "master": "yarn",
        "app_name": "YarnDataExtractor",
        "configs": {
            "spark.submit.deployMode": "client",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "5"
        }
    }
