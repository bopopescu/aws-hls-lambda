import pymysql
import json
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import os

import redis

env_dist = os.environ


def lambda_handler(event, context):
    print(event)
    snsMsg = json.loads(event['Records'][0]['Sns']['Message'])
    status = snsMsg['state']
    if status == 'PROGRESSING':
        return snsMsg['input']['key'] + 'create jop success! jobId: ' + snsMsg['jobId']
    elif status == 'ERROR':
        return event['Records'][0]['Sns']['Subject'] + ' key: ' + snsMsg['input']['key']
    elif status == 'COMPLETED':
        channelId = snsMsg['input']['key'].split('-')[1]
        outputKeyPrefix = snsMsg['outputKeyPrefix']
        if len(snsMsg['playlists']) > 1:
            print(channelId + " playlists more than 1")
            pass
        playListName = snsMsg['playlists'][0]['name']
        hlsPath = outputKeyPrefix + playListName + '.m3u8'

        db_host = env_dist['db_host']
        db_user = env_dist['db_user']
        db_passwd = env_dist['db_passwd']
        db_name = env_dist['db_name']
        db_port = int(env_dist['db_port'])
        db = pymysql.connect(db_host, db_user, db_passwd, db_name, db_port)
        cursor = db.cursor()
        sql = "SELECT id,mp4url,title,post_timeline,user_id FROM channel WHERE guid = '" + channelId + "'"
        print(sql)
        try:
            cursor.execute(sql)
            results = cursor.fetchone()
            if not results:
                return "not found channel data channelId:" + channelId + " hlsPath:" + hlsPath
            # post_timeline eq 1 send email
            mp4url = results[1]

            # title = results[2]
            # post_timeline = results[3]
            # user_id = results[4]
            # if post_timeline == 1:
            #     userSql = "SELECT email FROM user WHERE guid = '" + user_id + "'"
            #     cursor.execute(userSql)
            #     userInfo = cursor.fetchone()
            #     if userInfo[0]:
            #         send_mail(title=title + ' - ' + mp4url, receiver_mail=userInfo[0])
            #     pass
            upSql = "UPDATE channel SET hlsurl = '" + hlsPath + "' WHERE guid = '" + channelId + "'"
            print(upSql)
            cursor.execute(upSql)
            r = redis.Redis(host=env_dist['redis_host'], port=env_dist['redis_port'], db=0)
            p = r.pipeline()
            # p.hmset('broadcast:info:id:' + channelId, {'video_url': mp4url, 'hlsurl': env_dist['cdn_url'] + hlsPath})
            p.hmset('broadcast:info:id:' + channelId, {'video_url': env_dist['cdn_url'] + hlsPath})
            p.execute()
            db.commit()
        except:
            db.rollback()
            return "sql execute error channelId: " + channelId + " hlsPath:" + hlsPath
        # 关闭数据库连接
        db.close()
        return 'success'


def send_mail(title=None, receiver_mail=None, mail_msg=None):
    subject = title
    sender = 'info@kumu.ph'
    receivers = receiver_mail
    username = env_dist['username']
    passwd = env_dist['passwd']

    if not mail_msg:
        mail_msg = """
        <div>Thank you for hosting with Kumu. Here is a link to download your video</div>
        </br>
        <div>If you have any questions, please reply back and we will get back to you</div>
        """
    message = MIMEText(mail_msg, 'html', 'utf-8')
    # message['From'] = Header("发件人", 'utf-8')
    # message['To'] = Header("收件人", 'utf-8')

    subject = subject
    message['Subject'] = Header(subject, 'utf-8')

    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo()
        server.starttls()
        server.login(username, passwd)

        server.sendmail(sender, receivers, message.as_string())
        print("Mail Send Successfully")
        server.quit()

    except smtplib.SMTPException as err:
        print(err)
        print("Error:unable to send mail")
