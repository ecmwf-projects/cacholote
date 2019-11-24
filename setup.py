
import setuptools

setuptools.setup(
    name='callcache',
    version='0.1.0',
    url='https://github.com/bopen/callcache',
    author='Alessandro Amici',
    author_email='a.amici@bopen.eu',
    description='Efficiently cache calls to functions',
    packages=setuptools.find_packages(),
    install_requires=['heapdict', 'pymemcache'],
)
