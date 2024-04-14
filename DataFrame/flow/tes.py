# check
# anogram
str1 = "veda"
str2 = "vade"
count = 0
if len(str1) == len(str2):
    for i in str1:
        if i in str2:
            count = count + 1
if count == len(str1):
    print("Anagram")
else:
    print("not anagram")
# Write an SQL query to retrieve the names of employees who earn more than the average salary.
# select name from employees where salary>(select avg(salary) from employees);
# Write an SQL query to delete all records from the employees table who are receiving the salary less that average salary
# delete from employees where salary<(select avg(salary) from employees);
str="the mount everest is the everest"
s=str.split()
si=[]
count=0
for i in s:
    if i not in si:
        si.append(i)
        for j in range(len(s)):
            if i==s[j]:
                count+=1
        else:
            print(i,count,end=" ")
            count=0
