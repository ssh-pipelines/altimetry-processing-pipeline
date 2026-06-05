from setuptools import setup

setup(
    name="utilities",
    version="0.1.0",
    packages=["utilities"],
    install_requires=["s3fs", "boto3", "pyyaml"],
    include_package_data=True,
    package_data={"utilities": ["products.yaml", "sources/*.yaml", "targets.yaml"]},
)
