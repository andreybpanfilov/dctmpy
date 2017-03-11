import sys

from setuptools import setup

if sys.version_info < (2, 7):
    extras_require = {'nagios': ['argparse', 'nagiosplugin>=1.2.2']}
    install_requires = ["pyOpenSSL", "pyjks"]
else:
    extras_require = {'nagios': ['nagiosplugin>=1.2.2']}
    install_requires = ["pyjks"]

setup(
    name='dctmpy',
    version='0.3.2',
    packages=['dctmpy', 'dctmpy.net', 'dctmpy.obj', 'dctmpy.rpc', 'dctmpy.nagios'],
    package_dir={'': 'src'},
    license='ZPL-2.1',
    url='https://github.com/andreybpanfilov/dctmpy',
    author='Andrey B. Panfilov',
    author_email='andrew@panfilov.tel',
    description='Python bindings for Documentum',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Zope Public License',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Monitoring',
    ],
    zip_safe=False,
    extras_require=extras_require,
    entry_points={
        'console_scripts':
            ['nagios_check_docbase = dctmpy.nagios.check_docbase:main [nagios]',
             'nagios_check_docbroker = dctmpy.nagios.check_docbroker:main [nagios]']
    },
    install_requires=install_requires
)
