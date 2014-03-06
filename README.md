piazzaapi
=========

Python api to access and post data from piazza.

Adapted from https://gist.github.com/alexjlockwood/6797443

`piazza_api.py`: Allows getting data from a piazza course and writing to file,
elasticsearch, or printing to console.

* `--username`: The username to login with.
* `--password`: The password for the username.
* `--content_id`: The id of the desired content.
* `--course_id`: The id of the desired course.
* `--start_id`: The id to start writing the course data from.
* `--end_id`: The id to stop writing the course data at.
* `--data_file`: The file to output all course data when content_id is not 
provided.
* `--elasticsearch_host`: If provided will store raw data into elasticsearch.
* `--elasticsearch_index`: If provided will write data into this index.
* `--elasticsearch_type`: If provided will write data into this type.
* `--raw`: Print raw json data. Default is False.

more documentation in the file.
