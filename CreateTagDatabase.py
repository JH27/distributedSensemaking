import dbaccess
import csv
from pprint import pprint
import re


msqlu = 'root'
msqlp = 'qhshfl27'
msqldb = 'setags_ux'
msqlh = 'localhost'

acceso = dbaccess.get_Connection(msqlh, 3306, msqlu, msqlp, msqldb)



def getTags(rawtagstr):
    
    # split tags
    mlist = re.finditer("\<(?P<tag>[A-Za-z0-9\_\-]+)\>",rawtagstr)
    
    tags = []
    for m in mlist:
        tags.append(m.group('tag'))
    return tags

def createTagChangeDB(filename,tablename1,tablename2):
    
    # read in csv data
    reader = csv.reader(open(filename,"rU"))
    header = reader.next()
    
    for (i,row) in enumerate(reader):
        t = dict(zip(header,row))
        
    
        # check whether this is actually a tag modification
        tags = set(getTags(t['text']))
        ptags = set(getTags(t['prevtext']))
        
        if tags!=ptags:
            

            
            adds = tags.difference(ptags)
            removes = ptags.difference(tags)
            constants = tags.intersection(ptags)
            adds.discard("untagged")
            adds.discard("please-remove-this-tag")
            removes.discard("untagged")
            removes.discard("please-remove-this-tag")
            constants.discard("untagged")
            constants.discard("please-remove-this-tag")
            
            
            isreplace = "0"
            changetypeid = "0"
            if len(adds)>0 and len(removes)>0:
                isreplace = "1"
                changetypeid = "3"
            elif len(adds)>0:
                changetypeid = "1"
            elif len(removes)>0:
                changetypeid = "2"
            
            # save additions to tagedit table
            for a in adds:
                valStr = ",".join([t["id"],t["isqasker"],"1",isreplace,'"'+a+'"'])
                sqlStr = "INSERT INTO "+tablename2+" (posthistoryid,isqasker,edittypeid,replacetypeid,tagname) VALUES ("+valStr+")"
                acceso[1].execute(sqlStr)
                acceso[0].commit()                
            
            # save deletions to tagedit table
            for r in removes:
                valStr = ",".join([t["id"],t["isqasker"],"2",isreplace,'"'+r+'"'])
                sqlStr = "INSERT INTO "+tablename2+" (posthistoryid,isqasker,edittypeid,replacetypeid,tagname) VALUES ("+valStr+")"
                acceso[1].execute(sqlStr)
                acceso[0].commit()  
               
            # save constants to tagedit table 
            for c in constants:
                valStr = ",".join([t["id"],t["isqasker"],"0",isreplace,'"'+c+'"'])
                sqlStr = "INSERT INTO "+tablename2+" (posthistoryid,isqasker,edittypeid,replacetypeid,tagname) VALUES ("+valStr+")"
                acceso[1].execute(sqlStr)
                acceso[0].commit()  
            
            # only record edits that had an add or a remove    
            if len(adds)>0 or len(removes)>0:
                # save data to tagposthistory database
                valStr = ",".join([t["id"],t["postid"],'"'+t["creationdate"]+'"',t["userid"],t["posthistorytypeid"],t["isqasker"],changetypeid,'"'+t["text"]+'"','"'+t['prevtext']+'"'])
                sqlStr = "INSERT INTO "+tablename1+" (PostHistoryId,PostId,CreationDate,UserId,PostHistoryTypeId,IsQAsker,ChangeTypeId,Text,PrevText) VALUES ("+valStr+")"
                acceso[1].execute(sqlStr)
                acceso[0].commit()
                
#            if i < 100:
#                print(adds,removes)


def printSql(sqlquery,csvfile,header):

    writer = csv.writer(open(csvfile,"w"))
    writer.writerow(header)

    data = dbaccess.raw_query_SQL(acceso[1],sqlquery)
    for row in data:
        #print(row)
        writer.writerow(row)
        
def printSql2(sqlquery):


    data = dbaccess.raw_query_SQL(acceso[1],sqlquery)
    for row in data:
        print(row)

   
    
def getFreqAdditions(csvfile):
    
    sqlquery = """SELECT p.TagAdd,p.TagSame,a.freq,p.freq
                    FROM
                        (SELECT t1.TagName AS TagAdd, count(*) AS freq
                            FROM (SELECT * FROM co_TagEdits WHERE EditTypeId=1 AND ReplaceTypeId=0 AND IsQAsker=0) AS t1
                        GROUP BY t1.TagName) AS a
                    JOIN
                    (SELECT t1.TagName AS TagAdd, t2.TagName AS TagSame, count(*) AS freq
                        FROM (SELECT * FROM co_TagEdits WHERE EditTypeId=1 AND ReplaceTypeId=0 AND IsQAsker=0) AS t1
                        JOIN (SELECT * FROM co_TagEdits WHERE EditTypeId=0 AND IsQAsker=0) AS t2 ON t1.PostHistoryId=t2.PostHistoryId
                    GROUP BY t1.TagName,t2.TagName) AS p ON a.TagAdd=p.TagAdd
                    WHERE a.freq>=5 OR p.freq>=5
                    ORDER BY -a.freq,-p.freq

            """
    printSql(sqlquery,csvfile,["TagAdd","TagSame","TagAddFreq","PairFreq"])
    
def getFreqRemovals(csvfile):

    sqlquery = """SELECT n.TagRemove,n.freq
                    FROM
                    (SELECT t1.TagName AS TagRemove, count(*) AS freq
                        FROM (SELECT * FROM co_TagEdits WHERE EditTypeId=2 AND ReplaceTypeId=0 AND IsQAsker=0) AS t1
                    GROUP BY t1.TagName) AS n
                    WHERE n.freq>=5
                    ORDER BY -n.freq
                    
            """
    printSql(sqlquery,csvfile,["TagRemove","TagRemoveFreq"])
    
def getFreqReplacePairs(csvfile):
    sqlquery = """SELECT p.TagAdd,p.TagRemove,a.freq,p.freq
                    FROM
                        (SELECT t1.TagName AS TagAdd, t2.TagName AS TagRemove, count(*) AS freq
                            FROM (SELECT * FROM co_TagEdits WHERE EditTypeId=1 AND ReplaceTypeId=1 AND IsQAsker=0) AS t1
                            JOIN (SELECT * FROM co_TagEdits WHERE EditTypeId=2 AND IsQAsker=0) AS t2 ON t1.PostHistoryId=t2.PostHistoryId
                        GROUP BY t1.TagName,t2.TagName) AS p
                    JOIN
                        (SELECT t1.TagName AS TagRemove, count(*) AS freq
                                FROM (SELECT * FROM co_TagEdits WHERE EditTypeId=2 AND ReplaceTypeId=1 AND IsQAsker=0) AS t1
                            GROUP BY t1.TagName) AS a ON p.TagRemove=a.TagRemove
                    WHERE p.freq>=5 OR (a.freq>=5 AND p.freq>1)
                    ORDER BY -p.freq, -a.freq
            """
    printSql(sqlquery,csvfile,["TagAdd","TagRemove","TagRemoveFreq","PairFreq"])

def getFreqReplacePairs_add(csvfile):
    sqlquery = """SELECT p.TagAdd,p.TagRemove,a.freq,p.freq
                    FROM
                        (SELECT t1.TagName AS TagAdd, t2.TagName AS TagRemove, count(*) AS freq
                            FROM (SELECT * FROM co_TagEdits WHERE EditTypeId=1 AND ReplaceTypeId=1 AND IsQAsker=0) AS t1
                            JOIN (SELECT * FROM co_TagEdits WHERE EditTypeId=2 AND IsQAsker=0) AS t2 ON t1.PostHistoryId=t2.PostHistoryId
                        GROUP BY t1.TagName,t2.TagName) AS p
                    JOIN
                        (SELECT t1.TagName AS TagAdd, count(*) AS freq
                                FROM (SELECT * FROM co_TagEdits WHERE EditTypeId=1 AND ReplaceTypeId=1 AND IsQAsker=0) AS t1
                            GROUP BY t1.TagName) AS a ON p.TagAdd=a.TagAdd
                    WHERE p.freq>=5 OR (a.freq>=5 AND p.freq>1)
                    ORDER BY -p.freq, -a.freq
            """
    printSql(sqlquery,csvfile,["TagAdd","TagRemove","TagAddFreq","PairFreq"])

### get frequency of removals, additions, replacements
def countFreqEditType():
    sqlquery = """SELECT ChangeTypeId,count(*)
                    FROM co_TagPostHistory AS m
                    WHERE IsQAsker=0
                    GROUP BY ChangeTypeId
            """
    printSql2(sqlquery)   
    

def getRandomChanges(csvfile):
    sqlquery = """ SELECT h.PostHistoryId,IF(h.ChangeTypeId=1,'Add',IF(h.ChangeTypeId=2,'Remove','Replace')),a.Tags,r.Tags,c.Tags,CONCAT('https://cooking.stackexchange.com/questions/',h.PostId) AS Url
                    FROM co_TagPostHistory AS h
                    LEFT JOIN (SELECT PostHistoryId,GROUP_CONCAT(TagName) AS Tags FROM co_TagEdits WHERE EditTypeId=1 GROUP BY PostHistoryId) AS a ON a.PostHistoryId=h.PostHistoryId
                    LEFT JOIN (SELECT PostHistoryId,GROUP_CONCAT(TagName) AS Tags FROM co_TagEdits WHERE EditTypeId=2 GROUP BY PostHistoryId) AS r ON r.PostHistoryId=h.PostHistoryId
                    LEFT JOIN (SELECT PostHistoryId,GROUP_CONCAT(TagName) AS Tags FROM co_TagEdits WHERE EditTypeId=0 GROUP BY PostHistoryId) AS c ON c.PostHistoryId=h.PostHistoryId
                    ORDER BY rand()
                    LIMIT 25
            """
    printSql(sqlquery,csvfile,["PostHistoryId","ChangeType","AddTags","RemoveTags","BaseTags","Url"]) 

def getFrequentChanges(csvfile):
    sqlquery = """SELECT n.TargetTag,n.OldTag,IF(n.ChangeTypeId=1,'Add',IF(n.ChangeTypeId=2,'Remove','Replace')),n.Freq,CONCAT('https://cooking.stackexchange.com/questions/',n.PostId) AS Url 
                    FROM (SELECT t1.TagName AS TargetTag,t2.TagName AS OldTag,h.ChangeTypeId,count(*) AS Freq,max(h.PostId) as PostId
                    FROM
                        (SELECT * FROM co_TagEdits WHERE IsQAsker=0) AS t1
                        JOIN (SELECT * FROM co_TagEdits WHERE IsQAsker=0) AS t2 ON t1.PostHistoryId=t2.PostHistoryId AND t1.EditTypeId!=t2.EditTypeId
                        JOIN co_TagPostHistory AS h ON t1.PostHistoryId=h.PostHistoryId
                        WHERE (h.ChangeTypeId=1 AND t1.EditTypeId=1) 
                            OR (h.ChangeTypeId=2 AND t1.EditTypeId=2)
                            OR (h.ChangeTypeId=3 AND t1.EditTypeId=1 AND t2.EditTypeId=2)
                    GROUP BY t1.TagName,t2.TagName,h.ChangeTypeId) AS n
                    ORDER BY -n.Freq
                    LIMIT 100
            """
    printSql(sqlquery,csvfile,["TargetTag","Oldtag","ChangeType","PairFreq","Url"])    




def idConflictModerators(csvfile):
        
    sqlquery = """ SELECT h.PostId,h.PostHistoryId,h.UserId,IF(h.ChangeTypeId=1,'Add',IF(h.ChangeTypeId=2,'Remove','Replace')),a.Tags,r.Tags,c.Tags,CONCAT('https://cooking.stackexchange.com/questions/',h.PostId) AS Url
                    FROM co_TagPostHistory AS h
                    JOIN (SELECT h1.PostId
                    FROM co_TagPostHistory AS h1
                    JOIN co_TagPostHistory AS h2 ON h1.postid=h2.postid
                    WHERE h1.IsQAsker=0 AND h2.IsQAsker=0 AND h1.UserId!=h2.UserId GROUP BY h1.PostId) AS d ON h.PostId=d.PostId
                    LEFT JOIN (SELECT PostHistoryId,GROUP_CONCAT(TagName) AS Tags FROM co_TagEdits WHERE EditTypeId=1 GROUP BY PostHistoryId) AS a ON a.PostHistoryId=h.PostHistoryId
                    LEFT JOIN (SELECT PostHistoryId,GROUP_CONCAT(TagName) AS Tags FROM co_TagEdits WHERE EditTypeId=2 GROUP BY PostHistoryId) AS r ON r.PostHistoryId=h.PostHistoryId
                    LEFT JOIN (SELECT PostHistoryId,GROUP_CONCAT(TagName) AS Tags FROM co_TagEdits WHERE EditTypeId=0 GROUP BY PostHistoryId) AS c ON c.PostHistoryId=h.PostHistoryId
                    ORDER BY h.PostId,h.CreationDate
            """
    printSql(sqlquery,csvfile,["PostId","PostHistoryId","UserId","ChangeType","AddTags","RemoveTags","BaseTags","Url"]) 

### get a list of top 25 removals, 25 additions, 25 replacements
    
# createTagChangeDB("../Queries/AppleTagChanges.csv","co_TagPostHistory","co_TagEdits")
# getFreqAdditions("../Output/FrequentTagAdditions.csv")
# getFreqRemovals("../Output/FrequentTagRemovals.csv")
getFreqReplacePairs_add("../Output/FrequentTagReplacements_ux_add.csv")
# countFreqEditType()

# getFrequentChanges("../Output/FrequentTagChanges.csv")
# getRandomChanges("../Output/Random25TagChanges.csv")
# idConflictModerators("../Output/MultiTagChanges.csv")

    