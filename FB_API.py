import requests
import json
from BQ_Tables_API import Article_ID
from BQ_Tables_API import post_new
from BQ_Tables_API import post_keywords

All_ID = ['118807651515245', '1478518292360151','1525959604282263', '135703023300400', '903177319804689', '662024917260663', '216372331723870', '1250250741760280', '975502379179828', '162362413811576', '137479926325471', '113832525375329', '252556838139001','172348966125763','263612580409873', '330610973736083', '1383694918554214', '273287692810098','184899118190398','181561258523426'\
'387760834744344', '141877778469', '345640828864132','300213276829462', '248337985295437', '875609565812296', '651843528163084', '308416525948412', '220050085932', '449664581882455', '433004610088636', '645691978864686']
for i in range(len(All_ID)):
    ID = All_ID[i]
    Access_token = 'EAAFGucTJ9OMBAKqj6ZCUH7WwT0jv8rlYIKcLJT1n2zE1nZA1bjnD2efILRhfgvEWKbrYePdsZBK3CUyvvwnE6DsyQjZBUGeTVs0aEyRW6DH0twk67RgGdrg4GrW9wA1vlLYvDbeLDTMIVGr77JWsZARDLntbxUmQZD'
    stop_crawler_day = 90

    article_id_s = Article_ID.Article_ID(ID, Access_token, stop_crawler_day) ## post in 90 days
    post_new.post_new_table(article_id_s, Access_token) ## packing all posting articles and upload
    post_keywords.post_keywords_table(article_id_s, Access_token)




quit()

ID = '975502379179828'
comment = '?fields=posts.limit(3){created_time, id}'
link = '&access_token='
Access_token = 'EAACEdEose0cBANVVEkj1dna500G6hxiT14gZAfciapasi7V5l80xPg9pTz6ghPCpybjSdff6dZCPAPAKa6hSAgNuCqiAI7TY1E6T0PvxZCsEWizzZBpaZCAexNR69tavOQO3ZBVZCRhW9TKjVnxhNPDQKNgUmZBDlfVXN1vqbBnlgKYbJwqtKM8JJekJHTlKYsZBpOgaooDELSQZDZD'
url = 'https://graph.facebook.com/v2.11/' + ID +comment + link + Access_token


response = requests.get(url)
html = json.loads(response.text)

print(html['posts']['data'][0]['crea'])
quit()
message = html['posts']['data'][0]['message']
print(message.encode('utf-8'))
