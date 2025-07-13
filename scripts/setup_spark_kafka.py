#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Kafka Setup Script
Downloads and configures required JAR files for Spark-Kafka integration
"""

import os
import sys
import urllib.request
from pathlib import Path

def setup_spark_kafka_jars():
    """Download and setup Spark-Kafka integration JARs"""
    
    # Create jars directory
    jars_dir = Path("jars")
    jars_dir.mkdir(exist_ok=True)
    
    # Required JAR files
    jars = [
        {
            'name': 'spark-sql-kafka-0-10_2.12-3.5.0.jar',
            'url': 'https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar'
        },
        {
            'name': 'kafka-clients-3.4.0.jar', 
            'url': 'https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar'
        },
        {
            'name': 'spark-token-provider-kafka-0-10_2.12-3.5.0.jar',
            'url': 'https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar'
        },
        {
            'name': 'commons-pool2-2.11.1.jar',
            'url': 'https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar'
        }
    ]
    
    print("🔧 Setting up Spark-Kafka integration JARs...")
    
    for jar in jars:
        jar_path = jars_dir / jar['name']
        
        if jar_path.exists():
            print(f"✅ {jar['name']} already exists")
            continue
            
        print(f"⬇️ Downloading {jar['name']}...")
        try:
            urllib.request.urlretrieve(jar['url'], jar_path)
            print(f"✅ Downloaded {jar['name']}")
        except Exception as e:
            print(f"❌ Failed to download {jar['name']}: {e}")
            return False
    
    # Set SPARK_CLASSPATH environment variable
    jars_paths = [str(jars_dir / jar['name']) for jar in jars]
    spark_classpath = ":".join(jars_paths)
    
    print(f"\n🎯 Setup complete!")
    print(f"📂 JARs downloaded to: {jars_dir.absolute()}")
    print(f"\n📝 Add this to your environment:")
    print(f"export SPARK_CLASSPATH={spark_classpath}")
    
    return True

def create_spark_defaults():
    """Create spark-defaults.conf with Kafka packages"""
    
    spark_conf_dir = Path("conf")
    spark_conf_dir.mkdir(exist_ok=True)
    
    defaults_file = spark_conf_dir / "spark-defaults.conf"
    
    content = """# Spark Kafka Integration Configuration
spark.jars.packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
spark.sql.streaming.kafka.useDeprecatedOffsetFetching false
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.serializer org.apache.spark.serializer.KryoSerializer
"""
    
    with open(defaults_file, 'w') as f:
        f.write(content)
    
    print(f"✅ Created spark-defaults.conf at {defaults_file}")

if __name__ == "__main__":
    print("🚀 Spark-Kafka Setup")
    print("=" * 50)
    
    success = setup_spark_kafka_jars()
    if success:
        create_spark_defaults()
        print("\n🎉 Setup completed successfully!")
        print("\n💡 Now you can run the VectorStream pipeline:")
        print("   python -m src.main")
    else:
        print("\n❌ Setup failed!")
        sys.exit(1)
