try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import pipestat


setup(
    name='pipestat',
    version=pipestat.__version__,
    author=pipestat.__author__,
    author_email='timmyyuan021@126.com',
    description='Stat dataset via pipeline(use mongo aggregation syntax)',
    long_description=open('README.rst').read(),
    license='MIT',
    url='https://github.com/timmy21/pipestat',
    packages=['pipestat'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
)
