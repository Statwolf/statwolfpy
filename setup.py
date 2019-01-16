from setuptools import setup, find_packages

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

install_reqs = parse_requirements('deps.txt', session='hack')
reqs = [str(ir.req) for ir in install_reqs]

setup(name='statwolf',
        version='0.0.0',
        description='Statwolf SDK',
        author='Pasquale Boemio',
        author_email='pasquale.boemio@statwolf.com',
        license='MIT',
        packages=find_packages(),
        install_requires=reqs,
        zip_safe=False)
