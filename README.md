Usage:
S3ListObjects is a wrapper that deals with 1K listing limit in S3.
pass in params, same as to http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#listObjects-property

```
var taras_s3 = require("taras-s3");
taras_s3.S3ListObjects(s3, {'Bucket':"mybucket"},
                       function (err, ls) {
                                console.log(ls);
                       });
```
