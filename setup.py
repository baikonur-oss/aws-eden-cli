from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = [
    'aws-eden-core==0.1.0',
]

setup(
    name='aws_eden_cli',
    version='v0.1.2',
    license='MIT',
    author='Tamirlan Torgayev',
    author_email='torgayev@me.com',
    description='ECS Dynamic Environment Manager (eden) CLI',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=requirements,
    zip_safe=True,
    url='https://github.com/baikonur-oss/aws-eden-cli',
    project_urls={
        'Source Code': 'https://github.com/baikonur-oss/aws-eden-cli',
    },
    packages=find_packages(exclude=('test',)),
    scripts=['bin/eden'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
)
