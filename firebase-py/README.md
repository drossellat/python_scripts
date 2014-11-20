##### install & configure the script


```shell
git clone https://github.com/gholadr/python_scripts.git
cd python_scripts/firebase-py
virtualenv .
source ./bin/activate
pip install boto
pip install ijson
```

### AIM permissions policy

```json
		{
			"Sid": "",
			"Effect": "Allow",
			"Principal": {
				"AWS": "*"
			},
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::backup.mysquar.com",
				"arn:aws:s3:::backup.mysquar.com/*"
			]
		}

```

##### run the script


```shell
python firebase-data-extractor.py -id=AWS_ACCESS_KEY -secret=AWS_SECRET_KEY -path='path' -last=10-digit-unix-timestamp

```

##### run the script locally with local file


```shell
python firebase-data-extractor.py -id=0 -secret=0 -local=/path/to/local/file -path='path' -last=10-digit-unix-timestamp

```
