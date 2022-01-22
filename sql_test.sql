--How Many Messages are being sent every day ?

select createdat::TIMESTAMP::DATE, count(message) as messages from SPARK.PUBLIC.messages
group by 1 order by 1

--Are there any users that did not receive any message?

with CTE as (
select A.ID from SPARK.PUBLIC.users A
inner join SPARK.PUBLIC.MESSAGES B
on A.ID = B.ID )

select firstname + lastname from SPARK.PUBLIC.users
where ID not in ( select ID from CTE )


--How many active subscriptions do we have today?

select count(user_id) as active_subscriptions from SPARK.PUBLIC.subscription
where status = 'Active'


-- Are there users sending messages without an active subscription? (some extra context for you: in our apps only premium users can send messages).

select B.ID from SPARK.PUBLIC.users A
inner join SPARK.PUBLIC.MESSAGES B
on A.ID = B.ID
inner join SPARK.PUBLIC.subscription C
on A.ID = C.user_id
where C.status <> 'Active'

--Did you identified any inaccurate/noisy record that somehow could prejudice
--the data analyses? How to monitor it (SQL query)? Please explain how do you
--suggest to handle with this noisy data?

select b.user_id, a.firstname, a.lastname, b.status as "Subscription Status" from users a
inner join subscription b
on a.id = b.user_id
where b.createdat = ''

--Some users subscription is empty, we have to ignore them during our analysis or also we need to why such data is coming.
