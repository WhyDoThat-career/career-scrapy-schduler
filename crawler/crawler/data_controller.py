from datetime import datetime,date

def remove_blank(data) :
    return data.replace(' ','')

def remove_line_change(data) :
    return data.replace('\n','')

def remove_blank_all(data) :
    return data.replace('\n','').replace(' ','')

def remove_xa0(data) :
    return data.replace('\xa0',' ')

def style_image_parse(data) :
    return [url.split()[1].split('"')[1] for url in data]

def wave_split(data,remove,wave_type='~') :
    return data.replace(' ','').replace(',','').replace(remove,'').split(wave_type)

def arr2str(array) :
    if array is not None :
        return ','.join(array)
    else :
        return None

def control_deadline(data,separator='.') :
    try :
        return datetime.strptime(data,f'%Y{separator}%m{separator}%d').strftime('%Y-%m-%d')
    except :
        return None

def control_deadline_kakao(data) :
    if data == '영입종료시' :
        return None
    else :
        return remove_blank(data).replace('년','-').replace('월','-').replace('일','').replace('까지','')

def control_deadline_naver(data) :
    if data == '상시모집' :
        return None
    else :
        return control_deadline(data.split('~')[1])
        

def control_deadline_programmers(data) :
    tmp = data.split(' ')
    if tmp[0] == '상시' :
        return None
    else :
        print('!'*20)
        print(data)
        print(tmp)
        return tmp[3]

def control_skill_tag_naver(data) :
    data = ''.join(data).replace('\n','').replace('\t','').split('#')
    return ','.join([x.upper().replace(' ','') for x in data if x != ''])