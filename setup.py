from setuptools import setup

setup(
    name='mardiclient',
    version='0.1.0',    
    description='Client to interact with the MaRDI knowledge graph',
    url='https://github.com/mardi4nfdi/mardiclient',
    author='MaRDI TA5',
    author_email='accounts_ta5@mardi4nfdi.de',
    packages=['mardiclient'],
    install_requires=[
        "wikibaseintegrator"
    ],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',  
        'Operating System :: POSIX :: Linux',        
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
