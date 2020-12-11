import setuptools

__VERSION__ = None
with open("powa_collector/__init__.py", "r") as fh:
    for line in fh:
        if line.startswith('__VERSION__'):
            __VERSION__ = line.split('=')[1].replace("'", '').strip()
            break

requires = ['psycopg2']

setuptools.setup(
    name="powa-collector",
    version=__VERSION__,
    author="powa-team",
    license='Postgresql',
    author_email="rjuju123@gmail.com",
    description="PoWA collector, a collector for performing remote snapshot with PoWA",
    long_description="See https://powa.readthedocs.io/",
    long_description_content_type="text/markdown",
    url="https://powa.readthedocs.io/",
    packages=setuptools.find_packages(),
    install_requires=requires,
    scripts=["powa-collector.py"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: PostgreSQL License",
        "Operating System :: OS Independent",
        "Intended Audience :: System Administrators",
        "Topic :: Database :: Front-Ends"
    ],
)
