create database bigdata;

create table t_student
(
    id   int(11) not null auto_increment,
    name varchar(255) default null,
    age  int(11)      default null,
    primary key (id)
) engine = InnoDB
  auto_increment = 7
  default charset = 'utf8';

Insert into t_student
values ('1', 'jack', '18');
Insert into t_student
values ('2', 'tom', '19');
Insert into t_student
values ('3', 'rose', '20');
Insert into t_student
values ('4', 'tom', '19');
Insert into t_student
values ('5', 'jack', '18');
Insert into t_student
values ('6', 'rose', '21');