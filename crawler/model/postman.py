import smtplib
from email.mime.text import MIMEText
import json
from datetime import datetime

def load_key(key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key

def login_mail() :
    email_key = load_key('../keys/email_key.json')
    print('Sending email')
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.ehlo()      # say Hello
    smtp.starttls()  # TLS 사용시 필요
    smtp.login(email_key['email'], email_key['password'])

    return smtp

def send_mail(smtp,subject,text,recieve_arr) :
    msg = MIMEText(
        f'''
        Time : {str(datetime.now()).split('.')[0]}
        
        {text}
        ''')
    msg['Subject'] = '[Crawler]'+subject
    msg['To'] = 'whydothat.studio@gmail.com'
    smtp.sendmail('whydothat.studio@gmail.com', 
            ['whydothat.studio@gmail.com']+recieve_arr,
             msg.as_string())
