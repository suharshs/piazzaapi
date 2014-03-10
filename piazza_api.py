"""
API to pull course question answer data from Piazza.
Command line tool will retrieve content with the following flags specified.
--username: The username to login with.
--password: The password for the username.
--content_id: The id of the desired content.
--course_id: The id of the desired course.
--start_id: The id to start writing the course data from.
--end_id: The id to stop writing the course data at.
--data_file: The file to output all data when content_id is not provided.
--elasticsearch_host: If provided will store raw data into elasticsearch.
--elasticsearch_index: If provided will write data into this index.
--elasticsearch_type: If provided will write data into this type.
--raw: Print raw json data. Default is False.
"""

import argparse
from cookielib import CookieJar
import elasticsearch
import json
import urllib2
from util import get_aid


class AuthenticationError(Exception):
    """
    Error in authenicating at login.
    """


class PiazzaAPI:
  """
  Provides access to the Piazza REST API.
  """

  def __init__(self, user, password):
    """
    Authenticates and instantiates the PiazzaAPI object.
    """
    self.cookie_jar = CookieJar()
    self.url_opener = urllib2.build_opener(
        urllib2.HTTPCookieProcessor(self.cookie_jar))
    self.authenticate(user, password)

  def authenticate(self, user, password):
    """
    Logs in the user and stores the session cookie.
    """
    login_url = 'https://piazza.com/logic/api?method=user.login'
    login_data = ('{"method":"user.login","params":{"email":"%s","pass":"%s"}}'
                  % (user, password))
    login_response = json.loads(
      self.url_opener.open(login_url, login_data).read())
    if login_response['result'] != 'OK':
      raise AuthenticationError(
        'Authentication failed.\n%s' % login_response['result'])

  def get_raw_content(self, content_id, course_id):
    """
    Gets the raw json content for the content_id and course_id.
    """
    content_url = 'https://piazza.com/logic/api?method=get.content'
    content_data = ('{"method":"content.get","params":{"cid":"%s","nid":"%s"}}'
                    % (content_id, course_id))
    return json.loads(self.url_opener.open(content_url, content_data).read())

  def get_question_data(self, content_id, course_id):
    """
    Returns the data for the specified course_id and content_id.
    Returns None if the content type isn't a question.
    """
    content_response = self.get_raw_content(content_id, course_id)
    content = {}
    if not content_response['result']:
      content['error'] = 'Content_id out of range.'
    elif (not 'type' in content_response['result'] or
          content_response['result']['type'] != 'question'):
      content['error'] = 'Not a question.'
    elif (not 'status' in content_response['result'] or
          content_response['result']['status'] == 'deleted'):
      content['error'] = 'Content was deleted.'
    else:
      content['question'] = content_response['result']['history'][0]['content']
      content['question_upvotes'] = len(
        content_response['result']['tag_good_arr'])
      content['subject'] = content_response['result']['history'][0]['subject']
      content['cid'] = content_response['result']['id']
      content['tags'] = ' '.join(content_response['result']['tags'])
      content['followups'] = []
      for child in content_response['result']['children']:
        if child['type'] == 's_answer' and len(child['history']) > 0:
          content['s_answer'] = child['history'][0]['content']
          content['s_answer_upvotes'] = len(child['tag_endorse'])
        if child['type'] == 'i_answer' and len(child['history']) > 0:
          content['i_answer'] = child['history'][0]['content']
          content['i_answer_upvotes'] = len(child['tag_endorse'])
        if child['type'] == 'followup':
          followup_doc = {}
          followup_doc['uid'] = child.get('uid', 'ANON')
          followup_doc['content'] = child['subject']
          followup_doc['comments'] = []
          for comment in child['children']:
            followup_doc['comments'].append({
              'uid': comment.get('uid', 'ANON'),
              'content': comment['subject']
            })
          content['followups'].append(followup_doc)
    return content

  def write_course_question_data(self, course_id, output_file, start_id=1,
                                 end_id=4000):
    """
    Writes all question data from the specified course_id into output_file.
    One JSON object per line of the file.
    """
    content_id = start_id
    with open(output_file, 'a+') as f:
      while content_id <= end_id:
        content = self.get_question_data(content_id, course_id)
        print content_id
        content_id += 1
        if not 'error' in content:
          f.write(json.dumps(content) + '\n')

  def write_course_data_elasticsearch(self, es_hosts, es_index, es_type,
                                      course_id, start_id=1, end_id=4000):
    """
    Writes all raw data into the specified elasticsearch index and type.
    """
    es = elasticsearch.Elasticsearch(es_hosts)
    content_id = start_id
    while content_id <= end_id:
      print course_id, content_id
      content_response = self.get_raw_content(content_id, course_id)
      if not content_response['result']:
        pass  # Content_id out of range.
      elif (not 'type' in content_response['result'] or
            content_response['result']['type'] != 'question'):
        pass  # Not a question.
      elif (not 'status' in content_response['result'] or
            content_response['result']['status'] == 'deleted'):
        pass  #Content was deleted.
      else:
        es.index(index=es_index, doc_type=es_type, body=content_response)
      content_id += 1

  def post_answer(self, course_id, answer_text, answer_type, cid=None,
                          content_id=None):
    """
    Posts content to piazza. Should be used to post an answer to piazza.
    Must provide either cid or content_id.
    """
    if not cid:
      cid = self.get_raw_content(content_id, course_id)['result']['id']
    post_url = ('https://piazza.com/logic/api?method=content.answer&aid=%s'
                % get_aid())
    post_data = ('{"method":"content.answer","params":{"content":"%s",' + 
                 '"type":"%s","cid":"%s","revision":0,"anonymous":"no"}}'
                 % (answer_text, answer_type, cid))
    self.url_opener.open(post_url, post_data).read()

  def post_followup(self, course_id, followup_text, cid=None, content_id=None,
                    resolved=True):
    """
    Posts content to a piazza followup. Must provide either cid or content_id.
    """
    if not cid:
      cid = self.get_raw_content(content_id, course_id)['result']['id']
    post_url = ('https://piazza.com/logic/api?method=content.create&aid=%s'
                % get_aid())
    post_data = ('{"method":"content.create",' +
                 '"params":{"content":"","type":"followup","revision":0,' + 
                 '"anonymous":"no","nid":"%s","subject":"%s","cid":"%s"}}'
                 % (course_id,followup_text,cid))
    post_data = post_data.encode('ascii', 'ignore')
    followup_json = json.loads(self.url_opener.open(post_url, post_data).read())
    if not followup_json['result']:
      return
    followup_cid = followup_json['result']['id']

    if resolved:
      post_url = (
        'https://piazza.com/logic/api?method=content.mark_resolved&aid=%s'
        % get_aid())
      post_data = ('{"method":"content.mark_resolved",' +
                   '"params":{"cid":"%s","resolved":true}}' % followup_cid)
      self.url_opener.open(post_url, post_data).read()

  def post_followup_comment(self, course_id, comment_text, cid):
    """
    Posts a comment to an existing followup.
    """
    post_url = ('https://piazza.com/logic/api?method=content.create&aid=%s'
                % get_aid())
    post_data = ('{"method":"content.create",' +
                 '"params":{"content":"","type":"feedback","revision":0,' + 
                 '"anonymous":"no","nid":"%s","subject":"%s","cid":"%s"}}'
                 % (course_id,comment_text,cid))
    post_data = post_data.encode('ascii', 'ignore')
    self.url_opener.open(post_url, post_data).read()


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Get Piazza question data.')
  parser.add_argument('--username', help='The username to login with.',
                      required=True)
  parser.add_argument('--password', help='The password for the username.',
                      required=True)
  parser.add_argument('--content_id',
                      help='The id of the desired content. If not provided' +
                           ' all course_ids data will be written to data_file.',
                      default=None)
  parser.add_argument('--course_ids', help='The ids of the desired courses.',
                      required=True, nargs='+')
  parser.add_argument('--start_id',
                      help='The id to start writing the course data from.',
                      type=int, default=1)
  parser.add_argument('--end_id',
                      help='The id to stop writing the course data at.',
                      type=int, default=4000)
  parser.add_argument('--data_file',
                      help='The file to output all course data when' +
                           ' content_id is not provided.', default=None)
  parser.add_argument('--elasticsearch_hosts',
                      help='If provided will store data into elasticsearch.',
                      default=None, nargs='+')
  parser.add_argument('--elasticsearch_index',
                      help='If provided will store data into this es index.',
                      default=None)
  parser.add_argument('--elasticsearch_type',
                      help='If provided will store data into this es type.',
                      default=None)
  parser.add_argument('--raw', help='Print raw json data.', type=bool,
                      default=False)
  args = parser.parse_args()

  piazza_api = PiazzaAPI(args.username, args.password)
  if args.content_id:
    if args.raw:
      print json.dumps(
        piazza_api.get_raw_content(args.content_id, args.course_ids[0]))
    else:
      print json.dumps(
        piazza_api.get_question_data(args.content_id, args.course_ids[0]))
  elif (args.elasticsearch_hosts and args.elasticsearch_type and
        args.elasticsearch_index):
    for course_id in args.course_ids:
      piazza_api.write_course_data_elasticsearch(args.elasticsearch_hosts,
                                                 args.elasticsearch_index,
                                                 args.elasticsearch_type,
                                                 course_id,
                                                 start_id=args.start_id,
                                                 end_id=args.end_id)
  elif args.data_file:
    for course_id in args.course_ids:
      piazza_api.write_course_question_data(course_id, args.data_file,
                                            start_id=args.start_id,
                                            end_id=args.end_id)
