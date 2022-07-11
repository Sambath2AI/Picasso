import requests,re,json,random, boto3, os, datetime, pytz, sys, ast
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from dateutil.tz import tzlocal

local = tzlocal()
now = datetime.datetime.now()
now = now.replace(tzinfo = local)
tz = pytz.timezone('Pacific/Auckland')
dtcollected = re.sub(r"\s.*","",str(now.astimezone(tz)))
folderdate = now.astimezone(tz).strftime("%Y%m%d")

bucket_name = "picasso"

client = boto3.client('s3')
 
s3 = boto3.client('s3',
    endpoint_url = 'https://s3.us-west-1.wasabisys.com',
    aws_access_key_id = 'UKZ1YKRV3PQ9GAMQ61LE',
    aws_secret_access_key = 'tDaVCpsOUeV1ReSYW7PcRYq4K7F1A4qKwhwO6Wum'
)

Roomtype_array = []
Mealinclusion_array = []
ratetype_array = []
price_currency_array = []
Onsiterate_array = []
shopid_array = []
hotelcode_array = []
subjecthotelcode_array = []
websitename_array = []
Maxocc_array = []
dtcollected_array = []
Ratedate_array = []
LOS_array = []
Sourceurl_array = []
Statuscode_array = []

def insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,Ratedate,los,sourceurl,Statuscode):
    Roomtype_array.append(Roomtype)
    Onsiterate_array.append(Onsiterate)
    Mealinclusion_array.append(Mealinclusion)
    ratetype_array.append(ratetype)
    price_currency_array.append(price_currency)
    shopid_array.append(shopid)
    Maxocc_array.append(Maxocc)
    subjecthotelcode_array.append(subhotelcode)
    hotelcode_array.append(hotelcode)
    websitename_array.append(websitename)
    dtcollected_array.append(dtcollected)
    Ratedate_array.append(Ratedate)
    LOS_array.append(los)
    Sourceurl_array.append(sourceurl)
    Statuscode_array.append(Statuscode)
    # print(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,Ratedate,los,sourceurl)

def parquet_append(filepath:'Path' or str, df: pd.DataFrame) -> None:
    table_original_file = pq.read_table(source=filepath,  pre_buffer=False, use_threads=True, memory_map=True)
    table_to_append = pa.Table.from_pandas(df)
    table_to_append = table_to_append.cast(table_original_file.schema)
    handle = pq.ParquetWriter(filepath, table_original_file.schema)
    handle.write_table(table_original_file)
    handle.write_table(table_to_append)
    handle.close()

def clean(match,repl,strg):
    return re.sub(match,repl,str(strg))

def regMatch(patn, blk, cln=False,repl=["'","''"]):
    if cln: return clean(repl[0],repl[1],re.search(patn, blk).group(1) if re.search(patn, blk) else '')
    else: return re.search(patn, blk).group(1) if re.search(patn, blk) else ''
 
def jsonMatch(key,subj, cln=False,repl=["'","''"]):
    if key in subj:
        c = subj[key] if str(subj[key]) else ''
        return clean(repl[0], repl[1], c) if c and cln else c
    else: return ''

Spaceblock_regex_first = '(?s)<h3 class="full_hotel">\s*(.*?)</h3>'
Spaceblock_regex_second = '(?s)<p style="height: 33px; overflow: hidden;">(.*?)</p>'
Spaceblock_regex_third = '(?s)rel="this_hotel_is_not_bookable">\s*(.*?)<a'
Spaceblock_regex_four = '(?s)calendar_no_av.*?>\s*(.*?)\s*</'
Spaceblock_regex_five = '(?s)full_hotel">\s*(.*?)\s*<span'
Spaceblock_regex_six = '(?s)no_availability_banner_wrapper"\s*>.*?">(.*?)</'
Spaceblock_regex_seven = '(?s)class="bui-alert bui-alert--error bui-alert--lar.*?>\s*(.*?)\s*</div>'
Spaceblock_regex_eight = '<div class="bui-alert bui-alert--error".*?>\s*(.*?)\s*</p>\s*</div>'


def fetchrates(url, shopid, subhotelcode, hotelcode, proxyip, userid):
    try:
        if "https" not in url:
            url = clean("http","https",url)
        url = clean(r"PRP", ";", url)
        websitename = 'Booking'
        proxyip = "['"+clean(r"PP","','",proxyip)+"']"
        proxyip = ast.literal_eval(proxyip)
        RateDate = regMatch('checkin=(\d+\-\d+\-\d+).*?checkout', url)
        Checkin = datetime.datetime.strptime(str(RateDate),'%Y-%m-%d').strftime('%Y%m%d')
        LOS = (datetime.datetime.strptime(re.search(r"checkin=(\d+\-\d+\-\d+).*?checkout=(\d+\-\d+\-\d+)", url).group(2), "%Y-%m-%d") - datetime.datetime.strptime(re.search(r"checkin=(\d+\-\d+\-\d+).*?checkout=(\d+\-\d+\-\d+)", url).group(1), "%Y-%m-%d")).days
        prname = str(hotelcode)+'_'+str(Checkin)+'_.parquet'
        if not os.path.exists(prname):
            df = pd.DataFrame({"HotelCode": [''],"SubjectHotelcode":[''],"WebsiteName":[''],"dtcollected":[''],"RateDate":[''],"Los": [''],"RoomType": [''],"OnsiteRate": [''],"RateType": [''],"MealInclusion Type": [''],"MaxOccupancy": [''],"Sourceurl":[''],"Currency":[''],"Statuscode":['']})
            table = pa.Table.from_pandas(df)
            pq.write_table(table, where=(prname))
        head = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
        proxy1 = random.choice(proxyip)
        proxies = {"https": "http://%s"% proxy1}
        try:
            try:
                hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 5)
            except:
                try:
                    proxy1 = random.choice(proxyip)
                    proxies = {"https": "http://%s"% proxy1}
                    hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 10)
                except:
                    try:
                        proxy1 = random.choice(proxyip)
                        proxies = {"https": "http://%s"% proxy1}
                        hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 5)
                    except:
                        proxy1 = random.choice(proxyip)
                        proxies = {"https": "http://%s"% proxy1}
                        hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 10)
        except Exception as e:
            print('error:',e)
            return None
        
        if hml.status_code != 200:
            return None
        
        html = hml.content.decode('utf-8')
        
        if 'searchresults' in hml.url:
            url_check = url
            if regMatch('b_go_to_hp_by_hid.*?\?.*?label=(.*?);', html):
                label = 'label='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?label=(.*?);', html))+';'
            elif regMatch("b_label: '(.*?)'", html):
                label = 'label='+str(regMatch("b_label: '(.*?)'", html))+';'
            else:
                label = ''
            if regMatch('b_go_to_hp_by_hid.*?\?.*?sid=(.*?);', html):
                sid = 'sid='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?sid=(.*?);', html))+';'
            else:
                sid = ''
            if regMatch('b_go_to_hp_by_hid.*?\?.*?all_sr_blocks=(.*?);', html):
                all_sr_blocks = 'all_sr_blocks='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?all_sr_blocks=(.*?);', html))+';'
            else:
                all_sr_blocks = ''
            if regMatch('b_go_to_hp_by_hid.*?\?.*?highlighted_blocks=(.*?);', html):
                highlighted_blocks = 'highlighted_blocks='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?highlighted_blocks=(.*?);', html))+';'
            else:
                highlighted_blocks = ''
            if regMatch('b_go_to_hp_by_hid.*?\?.*?dest_id=(.*?);', html):
                dest_id = 'dest_id='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?dest_id=(.*?);', html))+';'
            else:
                dest_id = ''
            if regMatch('b_go_to_hp_by_hid.*?\?.*?hpos=(.*?);', html):
                hpos = 'hpos='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?hpos=(.*?);', html))+';'
            else:
                hpos = ''
            if regMatch('b_go_to_hp_by_hid.*?\?.*?ucfs=(.*?);', html):
                ucfs = 'ucfs='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?ucfs=(.*?);', html))+';'
            else:
                ucfs = ''
            if regMatch('b_go_to_hp_by_hid.*?\?.*?dest_type=(.*?);', html):
                dest_type = 'dest_type='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?dest_type=(.*?);', html))+';'
            else:
                dest_type = ''  
            if regMatch('b_go_to_hp_by_hid.*?\?.*?room1=(.*?);', html):
                room1 = 'room1='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?room1=(.*?);', html))+';'
            else:
                room1 = ''
            url_check = clean('\?','?'+str(label)+str(sid)+str(all_sr_blocks)+str(highlighted_blocks)+str(dest_id)+str(hpos)+str(ucfs)+str(dest_type)+str(room1),url_check)
            try:
                try:
                    hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 5)
                except:
                    try:
                        proxy1 = random.choice(proxyip)
                        proxies = {"https": "http://%s"% proxy1}
                        hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 10)
                    except:
                        try:
                            proxy1 = random.choice(proxyip)
                            proxies = {"https": "http://%s"% proxy1}
                            hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 5)
                        except:
                            proxy1 = random.choice(proxyip)
                            proxies = {"https": "http://%s"% proxy1}
                            hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 10)
            except Exception as e:
                return None
            if hml.status_code != 200:
                return None
        html = hml.content.decode('utf-8')
        
        if regMatch(r'selected_currency=(.*?);', url):
            url_currency = regMatch(r'selected_currency=(.*?);', url)
        else:
            url_currency = ''
    
        Roomtype = Mealinclusion = ratetype = price_currency = ''
        Onsiterate = Maxocc = statuscode =0
        hid = re.sub(r'http.*?hotelid=', '', url)
        if not re.search("hotel_id\s*:\s*'"+str(hid)+"',", html):
            if hml.history:
                if (str(hml.history[0]) == '<Response [301]>' or str(hml.history[0]) == '<Response [302]>') and 'searchresults' in hml.url:
                    statuscode = 203
                    insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
            if hotelcode_array==[]:
                hotelid = regMatch(r"b_hotel_id: '(.*?)'",html)
                url = clean('hotelid=\d+', 'hotelid='+str(hotelid), url)
                
                proxy1 = random.choice(proxyip)
                proxies = {"https": "http://%s"% proxy1}
                try:
                    try:
                        hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 5)
                    except:
                        try:
                            proxy1 = random.choice(proxyip)
                            proxies = {"https": "http://%s"% proxy1}
                            hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 10)
                        except:
                            try:
                                proxy1 = random.choice(proxyip)
                                proxies = {"https": "http://%s"% proxy1}
                                hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 5)
                            except:
                                proxy1 = random.choice(proxyip)
                                proxies = {"https": "http://%s"% proxy1}
                                hml = requests.get(url, headers=head, proxies = proxies, verify = False, timeout = 10)
                except Exception as e:
                    return None
                if hml.status_code != 200:
                    return None
                html = hml.content.decode('utf-8')
                
                if 'searchresults' in hml.url:
                    url_check = url
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?label=(.*?);', html):
                        label = 'label='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?label=(.*?);', html))+';'
                    elif regMatch("b_label: '(.*?)'", html):
                        label = 'label='+str(regMatch("b_label: '(.*?)'", html))+';'
                    else:
                        label = ''
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?sid=(.*?);', html):
                        sid = 'sid='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?sid=(.*?);', html))+';'
                    else:
                        sid = ''
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?all_sr_blocks=(.*?);', html):
                        all_sr_blocks = 'all_sr_blocks='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?all_sr_blocks=(.*?);', html))+';'
                    else:
                        all_sr_blocks = ''
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?highlighted_blocks=(.*?);', html):
                        highlighted_blocks = 'highlighted_blocks='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?highlighted_blocks=(.*?);', html))+';'
                    else:
                        highlighted_blocks = ''
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?dest_id=(.*?);', html):
                        dest_id = 'dest_id='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?dest_id=(.*?);', html))+';'
                    else:
                        dest_id = ''
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?hpos=(.*?);', html):
                        hpos = 'hpos='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?hpos=(.*?);', html))+';'
                    else:
                        hpos = ''
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?ucfs=(.*?);', html):
                        ucfs = 'ucfs='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?ucfs=(.*?);', html))+';'
                    else:
                        ucfs = ''
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?dest_type=(.*?);', html):
                        dest_type = 'dest_type='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?dest_type=(.*?);', html))+';'
                    else:
                        dest_type = ''  
                    if regMatch('b_go_to_hp_by_hid.*?\?.*?room1=(.*?);', html):
                        room1 = 'room1='+str(regMatch('b_go_to_hp_by_hid.*?\?.*?room1=(.*?);', html))+';'
                    else:
                        room1 = ''
                    url_check = clean('\?','?'+str(label)+str(sid)+str(all_sr_blocks)+str(highlighted_blocks)+str(dest_id)+str(hpos)+str(ucfs)+str(dest_type)+str(room1),url_check)
                    try:
                        try:
                            hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 5)
                        except:
                            try:
                                proxy1 = random.choice(proxyip)
                                proxies = {"https": "http://%s"% proxy1}
                                hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 10)
                            except:
                                try:
                                    proxy1 = random.choice(proxyip)
                                    proxies = {"https": "http://%s"% proxy1}
                                    hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 5)
                                except:
                                    proxy1 = random.choice(proxyip)
                                    proxies = {"https": "http://%s"% proxy1}
                                    hml = requests.get(url_check, headers=head, proxies = proxies, verify = False, timeout = 10)
                    except Exception as e:
                        return None
                    if hml.status_code != 200:
                        return None
                html = hml.content.decode('utf-8')
                
                if regMatch(r'selected_currency=(.*?);', url):
                    url_currency = regMatch(r'selected_currency=(.*?);', url)
                else:
                    url_currency = ''
        if hotelcode_array==[]:
            if 'searchresults' in hml.url:
                statuscode = 206
                insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
              
            elif re.compile(Spaceblock_regex_third).findall(html):
                props =  re.search(Spaceblock_regex_third, str(html)).group(1)
                if "taking reservations on our site right now" in props:
                    statuscode = 203
                    insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
        
            elif "This property isn't taking reservations on our site right now" in html  or 'temporarily unavailable on our site, but we found' in html or 'There are no rooms available at this property' in html or 'isnt bookable on our site anymore, but we found some great alternatives for you' in html or "Unfortunately it's not possible to make reservations for this hotel at this time" in html:   
                statuscode = 203
                insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                
            elif re.compile(Spaceblock_regex_first).findall(html):
                if re.compile(Spaceblock_regex_first).findall(html):
                    statuscode = 202
                    insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
            elif "For your check-in date, there's a minimum length of stay of" in html:
                statuscode = 202
                insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
            
            elif "Minimum stay for check-in date is" in html:
                statuscode = 202
                insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
            else:
                if re.compile(r"(?s)b_rooms_available_and_soldout:\s*(.*?\])\s*,\s*b_.*?photo_pid:").findall(html):
                    Curr_reg = re.search("b_selected_currency:\s*'(.*?)'", html) 
                    if Curr_reg: 
                        price_currency = re.sub("'", "''", Curr_reg.group(1)) 
                    else: 
                        currency_reg = re.search(r'hidden" name="selected_currency" value="(.*?)" />', html) 
                        if currency_reg: 
                            price_currency = currency_reg.group(1)
                    if str(url_currency.lower()) != str(price_currency.lower()):
                        return None
                    
                    mealplandict = {}
                    for mealblock in re.compile(r'(?s)<tr data-block-id=".*?</select>\s*</div>\s*</td>\s*</tr>').findall(html):
                        datablockid = re.search(r'data-block-id="(.*?)"', mealblock).group(1)
                        mealplan=''
                        mealcheck   = re.search(r'(?s)<span class="bicon-\w+ mp-icon meal-plan-icon.*?>(.*?)</li>',mealblock)
                        mealplan    = mealcheck.group(1) if mealcheck else ""
                        if mealplan=="":
                            mealcheck   = re.search(r'(?s)<svg class="bk-icon -streamline-food_coffee".*?>.*?<div class="bui-list__description">\s*(.*?)\s*</div>',mealblock)
                        mealplan    = mealcheck.group(1) if mealcheck else ""
                        if mealplan=="":
                            mealcheck   = re.search(r'(?s)<div class="bui-list__description">\s*<span class="bui-text--color-constructive">\s*(.*?)\s*</span>',mealblock)
                        mealplan    = mealcheck.group(1) if mealcheck else ""
                        mealplan=re.sub(r"(?si)<.*?>", r"", mealplan)
                        mealplandict[datablockid] = mealplan 
                    for json_block in re.compile(r"(?s)b_rooms_available_and_soldout:\s*(.*?\])\s*,\s*b_.*?photo_pid:").findall(html):
                        # print(json_block)
                        json_blk = re.sub(r"'","''",str(json_block))
                        js_data = json.loads(json_blk)
                        # print(js_data)
                        for datas in js_data:
                            if 'b_name' in datas:
                                Roomtype = datas['b_name']
                                # print('Roomtype:',Roomtype)
                            else:
                                Roomtype = ''
                            for b_block in datas['b_blocks']:
                                b_block_id = b_block['b_block_id']
                                
                                if 'b_max_persons' in b_block:
                                    Maxocc = b_block['b_max_persons']
                                else:
                                    Maxocc = 0
                                
                                if 'b_price' in b_block:
                                    Onsiterate = b_block['b_price']
                                    Onsiterate = re.sub(r"&.*?;","",re.sub(r"(?s)<.*?>|=", r"", re.sub("Rs\.|USD\.|Usd\.|z&#x0142;|163;|20AC;|[A-Za-z]|&nbsp;|,|\$|'|&#269;|165;", "", str(Onsiterate).encode('ascii','ignore').decode('utf-8')))).strip()
                                else:
                                    Onsiterate = 0
                                    
                                if 'b_mealplan_included_name' in b_block:
                                    Mealinclusion = b_block['b_mealplan_included_name']
                                    if Mealinclusion=='':
                                        Mealinclusion = re.sub("\s+", " ", re.sub("<.*?>","", mealplandict[b_block_id]))
                                else:
                                    Mealinclusion = ''
                                    
                                if 'b_cancellation_type' in b_block:
                                    ratetype = b_block['b_cancellation_type']
                                else:
                                    ratetype = ''
                                
                                if Onsiterate:
                                    statuscode = 200
                                else:
                                    statuscode = 201
                                insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                                    
                else:
                    statuscode = 202
                    if re.compile(Spaceblock_regex_second).findall(html):
                        insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                    elif re.compile(Spaceblock_regex_third).findall(html):
                        insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                    elif re.compile(Spaceblock_regex_four).findall(html):
                        insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                    elif re.compile(Spaceblock_regex_five).findall(html):
                        insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                    elif re.compile(Spaceblock_regex_six).findall(html):
                        insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                    elif re.compile(Spaceblock_regex_seven).findall(html):
                        insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                    elif re.compile(Spaceblock_regex_eight).findall(html):
                        insert(Roomtype,Onsiterate,Mealinclusion,ratetype,price_currency,shopid,Maxocc,subhotelcode,hotelcode,websitename,dtcollected,RateDate,LOS,url,statuscode)
                    else:
                        return None
        if hotelcode_array!=[]:
            mydata = {"HotelCode": hotelcode_array,"SubjectHotelcode":subjecthotelcode_array,"WebsiteName":websitename_array,"dtcollected":dtcollected_array,"RateDate":Ratedate_array,"Los": LOS_array,"RoomType": Roomtype_array,"OnsiteRate": Onsiterate_array,"RateType": ratetype_array,"MealInclusion Type": Mealinclusion_array,"MaxOccupancy": Maxocc_array,"Sourceurl": Sourceurl_array,"Currency":price_currency_array,"Statuscode":Statuscode_array}
            df = pd.DataFrame(mydata)
            parquet_append(prname, df)
            s3.upload_file(f'{prname}', bucket_name, f'{folderdate}/{userid}/{shopid}/{prname}')
            os.remove(prname)
        else:
            return None
    except Exception as e:
        value_error = str(re.sub(r"'", '"', str(e)))
        stacktrace = sys.exc_info()[2].tb_lineno
        print('value_error:',value_error, stacktrace)
        return 204


script, proxyip, shopid, subhotelcode, hotelcode, url, userid=sys.argv
# url = 'https://www.booking.com/hotel/eg/jungle-park.en-gb.html?checkin=2022-08-21;checkout=2022-08-28;group_adults=1;lang=en-us;selected_currency=EUR;changed_currency=1;hotelid=267353'
# shopid = 1
# subhotelcode = 2
# hotelcode = 3
# proxyip = ['media:med!a@50.3.89.3:12345', 'media:med!a@95.181.219.211:12345', 'media:med!a@104.160.14.114:12345', 'media:med!a@95.181.218.242:12345', 'media:med!a@196.196.254.50:12345', 'media:med!a@196.196.220.155:12345', 'media:med!a@196.196.220.204:12345', 'media:med!a@181.214.89.129:12345', 'media:med!a@196.196.254.51:12345', 'media:med!a@185.46.116.224:12345', 'media:med!a@181.214.89.127:12345', 'media:med!a@5.157.29.14:12345', 'media:med!a@50.3.89.91:12345', 'media:med!a@181.214.89.77:12345', 'media:med!a@50.3.89.4:12345', 'media:med!a@95.181.217.63:12345', 'media:med!a@185.46.119.166:12345', 'media:med!a@5.157.23.227:12345', 'media:med!a@165.231.130.32:12345', 'media:med!a@95.181.218.111:12345', 'media:med!a@50.3.89.203:12345', 'media:med!a@165.231.130.68:12345', 'media:med!a@50.3.91.5:12345', 'media:med!a@181.214.89.68:12345', 'media:med!a@165.231.130.130:12345', 'media:med!a@185.46.118.151:12345', 'media:med!a@185.46.117.170:12345', 'media:med!a@196.196.169.200:12345', 'media:med!a@185.46.117.179:12345', 'media:med!a@5.157.29.161:12345', 'media:med!a@50.3.136.140:12345', 'media:med!a@50.3.89.8:12345', 'media:med!a@50.3.91.187:12345', 'media:med!a@165.231.130.141:12345', 'media:med!a@5.157.29.193:12345', 'media:med!a@165.231.225.111:12345', 'media:med!a@165.231.227.133:12345', 'media:med!a@196.196.254.207:12345', 'media:med!a@165.231.130.210:12345', 'media:med!a@95.181.219.209:12345', 'media:med!a@104.160.14.214:12345', 'media:med!a@104.160.14.113:12345', 'media:med!a@104.160.14.170:12345', 'media:med!a@196.196.220.251:12345', 'media:med!a@196.196.220.159:12345', 'media:med!a@196.196.254.38:12345', 'media:med!a@181.214.89.84:12345', 'media:med!a@5.157.29.59:12345', 'media:med!a@95.181.218.33:12345', 'media:med!a@95.181.217.74:12345']
#
fetchrates(url, shopid, subhotelcode, hotelcode, proxyip, userid)        
            






