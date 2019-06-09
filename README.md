# wapy-lambda-handlers

The lambda handlers will manage and post the data we are getting from the camera service.

From the camera service we are getting images and JSON data.

The objects handler will get the JSON data with the object the person was looking and some data about the store and user
and post it to the SQL database in the right table.

The images handler will get the image we stored on S3 and send it to the Rekognition service, we will get the emotions,
age range and gender about the person. the handler will post this data also to the SQL database.

