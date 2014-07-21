##### install & configure the script


```shell
git clone https://github.com/gholadr/python_scripts.git
cd python_scripts/firebase-py
virtualenv .
source ./bin/activate
pip install requests
pip install python-firebase
```

##### run the script


```shell
python firebase-data-extractor.py -key=firebase-secret-key -dsn='https://your-app.firebaseio.com' -email='your@account.com' -path='path' -last=10-digit-unix-timestamp

```

