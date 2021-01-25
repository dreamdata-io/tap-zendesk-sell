from setuptools import setup


setup(
    name="tap-zendesk-sell",
    version="0.1",
    description="Dreamdata.io tap for extracting data from the Zendesk Sell API",
    author="Dreamdata",
    author_email="engineering@dreamdata.io",
    url="http://dreamdata.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_zendesk_sell"],
    install_requires=[
        "pydantic==1.5.1",
        "requests==2.22.0",
    ],
    entry_points="""
          [console_scripts]
          tap-zendesk-sell=tap_zendesk_sell.main:main
      """,
    packages=[
        "tap_zendesk_sell",
    ],
    include_package_data=True,
)
