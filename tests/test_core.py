import os

# Flintrock
from flintrock.core import (
    generate_template_mapping,
    get_formatted_template,
)

FLINTROCK_ROOT_DIR = (
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__))))


def test_templates(dummy_cluster):
    template_dir = os.path.join(FLINTROCK_ROOT_DIR, 'flintrock', 'templates')
    for (dirpath, dirnames, filenames) in os.walk(template_dir):
        if filenames:
            for filename in filenames:
                template_path = os.path.join(dirpath, filename)
                mapping = generate_template_mapping(
                    cluster=dummy_cluster,
                    hadoop_version='',
                    spark_version='',
                    spark_executor_instances=0,
                )
                get_formatted_template(
                    path=template_path,
                    mapping=mapping,
                )
