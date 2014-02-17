from distutils.core import setup

setup(
    name='dctmpy',
    version='current',
    packages=['dctmpy', 'dctmpy.net', 'dctmpy.obj', 'dctmpy.nagios'],
    package_dir={'': 'src'},
    url='https://github.com/andreybpanfilov/dctmpy',
    license='',
    author='Andrey B. Panfilov',
    author_email='andrew@panfilov.tel',
    description='Python bindings for Documentum', requires=['nagiosplugin']

)
