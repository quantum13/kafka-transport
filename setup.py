from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='kafka-transport',
      version='0.3.0',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='https://github.com/Karma-blockchain/kafka-transport',
      author='Nozdrin-Plotnitsky Nikolay',
      author_email='nozdrin.plotnitsky@karma.red',
      license='MIT',
      packages=find_packages(),
      install_requires=[
        "msgpack >= 0.5.6",
        "aiokafka >= 0.4.2"
      ])
