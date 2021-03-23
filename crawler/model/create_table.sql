use crawl_job;
show tables;
drop table if exists job_detail;
create table job_detail (
    id int unsigned not null auto_increment primary key,
    crawl_date datetime,
    title varchar(500) not null,
    href varchar(500),
    main_text text not null,
    salary varchar(50),    
    skill_tag varchar(500),
    sector varchar(200),
    newbie boolean,
    career varchar(50),
    deadline date,
    company_name varchar(100),
    company_address varchar(500),
    big_company boolean,
    platform varchar(100),
    logo_image varchar(500)
) DEFAULT CHARSET=utf8mb4; 
desc job_detail;
select * from job_detail;
DELETE FROM job_detail WHERE platform = "programmers"