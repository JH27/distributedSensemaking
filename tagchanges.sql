
-- tag changes 관련 정보 다 긁어오는 쿼리

with t as (select postid, posthistorytypeid, case when userid is NULL then 0 else userid end as userid_withdel from posthistory)

Select n.id,n.postid,n.creationdate,n.userid,n.posthistorytypeid,n.isqasker,n.text,n.prevtext from 

(select h.id,h.postid,h.creationdate,h.userid,h.posthistorytypeid,CASE WHEN o.userid=h.userid THEN 1 ELSE 0 END AS isqasker,h.text, 
CASE WHEN h.posthistorytypeid=3 THEN NULL ELSE lag(h.text) over (order by h.postid,h.creationdate) END as prevtext 
from posthistory as h 

join 

-- 원래 교수가 보내줬던 코드. 이렇게 할 경우 deleted user가 생성한 포스트에서 발생한 tag changes가 잡히지 않는다는 단점이 있습니당
-- (select t.postid,count(*) AS numchanges from posthistory as t 
-- join posts as q on t.postid=q.id 
-- where t.posthistorytypeid=6 
-- and q.posttypeid=1 
-- and t.userid!=q.owneruserid 
-- group by t.postid) as p on h.postid=p.postid
-- 이 코드로 돌리면 최종 결과가 24498 건 나옴. 

-- 교수 코드를 아래같이 수정
(select t.postid,count(*) AS numchanges from t 
join (select id, posttypeid, case when owneruserid is NULL then 0 else owneruserid end as owneruserid_withdel from posts) as q on t.postid=q.id 
where t.posthistorytypeid=6 
and q.posttypeid=1 
and t.userid_withdel!=q.owneruserid_withdel 
group by t.postid) as p on h.postid = p.postid
-- 이 코드로 돌리면 최종 결과가 25086 건 나옴.

join 

(select * from posthistory where posthistorytypeid=3) as o on h.postid=o.postid 

where h.posthistorytypeid in (3,6,9)) 

AS n 
where n.posthistorytypeid in (6,9);

-- 내코드로 돌렸더니 전체 sql 돌리면 30652 건이 나오네.. 왜 25086과 차이가 나는거지?? 여튼 지금까지 테스트 한거 중에서는 이게 레코드가 젤 많이 보존됨. 