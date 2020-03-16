SET GLOBAL log_bin_trust_function_creators = 1;

DROP FUNCTION IF EXISTS randn;
DELIMITER //
CREATE FUNCTION randn(mean float, std float) RETURNS float
BEGIN
set @x=rand(), @y=rand();
set @gauss = ((sqrt(-2*log(@x))*cos(2*pi()*@y))*std)+mean;
return @gauss;
END
//
DELIMITER ;

DROP procedure IF EXISTS mode;
DELIMITER //
CREATE procedure mode(tables varchar(30), attr varchar(30))
BEGIN
set @tbl_n = tables;
set @atr_n = attr;
# a mode must occurs at least twice
drop table if exists counts;
set @s_dc = concat('CREATE TEMPORARY TABLE counts SELECT `', @atr_n, '`, COUNT(`' ,@atr_n, '`) AS freq FROM ',@tbl_n,' GROUP BY `',@atr_n,'` HAVING COUNT(*) > 1 ORDER BY freq DESC');
prepare stmt from @s_dc;
execute stmt;
SELECT MAX(freq) into @mode FROM counts;
# there may be more than one mode, pick one randomly

SET @s_temp = concat('SELECT `',@atr_n ,'` into @temp FROM counts WHERE freq = @mode limit 1');
prepare stmt2 from @s_temp;
execute stmt2;
DROP TABLE IF EXISTS counts;

set @s_r = concat('update ',@tbl_n, ' set `', @atr_n, '` = ',@temp, ' where `', @atr_n, '` is null');
prepare stmt1 from @s_r;
execute stmt1;
END//

DELIMITER ;


delimiter //
drop procedure if exists Clean_null_Values;
create procedure Clean_null_Values(tbl_n varchar(100), atr_n varchar(100), null_value_v varchar(100))

begin
set @tbl_n = tbl_n;
set @atr_n = atr_n;
set @null_value_v = null_value_v;

set @s_dc = concat('alter table ', @tbl_n, ' drop column `', @atr_n, '`');
set @s_rz = concat('update ', @tbl_n, ' set `', @atr_n, '` = 0 ', 'where `', @atr_n, '` is null');
set @s_dr = concat('delete from ', @tbl_n, ' where `', @atr_n, '` is null');
set @s_ra = concat('update  ', @tbl_n,  ' set  `',@atr_n, '` = (SELECT avg( `',@atr_n,'` ) from (select `',@atr_n,'` from ',@tbl_n,' WHERE `',@atr_n,'` is not null) t ) where `',@atr_n,'` is null');
set @s_else = concat('update ', @tbl_n, ' set `', @atr_n, '` = "', @null_value_v, '" where `', @atr_n, '` is null');
set @s_mean = concat('SELECT avg( `',@atr_n,'` ) into @mean from (select `',@atr_n,'` from ',@tbl_n,' WHERE `',@atr_n,'` is not null) t ');
set @s_std = concat('SELECT stddev( `',@atr_n,'` ) into @std from (select `',@atr_n,'` from ',@tbl_n,' WHERE `',@atr_n,'` is not null) t ');
set @s_rn = concat('update  ', @tbl_n,  ' set  `',@atr_n, '` =  randn(@mean,@std) where `',@atr_n,'` is null');
set @s_alter = concat('alter table ', @tbl_n , ' modify `', @atr_n , '` varchar(100)');



if @null_value_v = 'dc' then
begin
prepare stmt from @s_dc;
execute stmt;
end;

elseif @null_value_v = 'rz' then
begin
prepare stmt from @s_rz;
execute stmt;
end;

elseif @null_value_v = 'dr' then
begin
prepare stmt from @s_dr;
execute stmt;
end;

elseif @null_value_v = 'ra' then
begin
prepare stmt from @s_ra;
execute stmt;
end;


elseif @null_value_v = 'rn' then
begin
prepare stmt4 from @s_mean;
execute stmt4;
prepare stmt5 from @s_std;
execute stmt5;
prepare stmt6 from @s_rn;
execute stmt6;
end;


elseif @null_value_v = 'rm' then
begin
call mode(@tbl_n,@atr_n);
end;


elseif @null_value_v is null then
begin
end;

else
begin
prepare stmt1 from @s_alter;
execute stmt1;
prepare stmt from @s_else;
execute stmt;
end;

end if;

end //

delimiter ;



drop procedure if exists clean;

delimiter //

create procedure clean(tbl_n varchar(30))
begin
    
    -- execute projection
    drop table if exists temp_clean_table_1;
    set @proj_attr = (select GROUP_CONCAT(concat('`',attr_name,'`')) as 'tmp' from md_proj where table_name = tbl_n);
    set @proj_table = concat('create table temp_clean_table_1 select ', @proj_attr, ' from ', tbl_n);
    prepare stmt from @proj_table;
    execute stmt;    

    drop table if exists temp_md_table;
    CREATE TABLE if not exists temp_md_table
        SELECT * FROM md_attr WHERE table_name = tbl_n;


    set @num_of_attr = (select count(*) from temp_md_table);
    set @i = 1;
    set @table_name_v = 'temp_clean_table_1';

    while (@i <= @num_of_attr) do
        
        set @attr_name_v = (select attr_name from temp_md_table limit 1);
        set @null_value_v = (select null_value from temp_md_table limit 1);
        set @end_type_v = (select end_type from temp_md_table limit 1);

        -- determine the end type of columns
        if @end_type_v is not null then
        begin
        set @s = concat('alter table ', @table_name_v , ' modify `', @attr_name_v , '` ', @end_type_v);
        prepare stmt from @s;
        execute stmt;
        end;
        end if;

        -- dealing with null values
        if @null_value_v is not null then
        begin
        call Clean_null_Values(@table_name_v, @attr_name_v, @null_value_v);
        end;
        end if;




        DELETE from temp_md_table where attr_name = @attr_name_v;
        set @i = @i + 1;
    
    end while;
    
    -- implement logic constraints
    drop table if exists temp_clean_table_2;

    set @logic_stmts = (select GROUP_CONCAT(tmp SEPARATOR ' and ') from
        (select case when back in (select attr_name from md_proj where table_name = tbl_n)
        then concat('`',front,'` ', equality, ' `',back,'`')
        else concat('`',front,'` ', equality, ' ', back)
        end as tmp
        from md_logic where table_name = tbl_n) a 
        );

    set @logic_table = concat('create table temp_clean_table_2 select * from temp_clean_table_1 where ', @logic_stmts);
    prepare stmt from @logic_table;
    execute stmt;

    -- implement sorting
    drop table if exists temp_clean_table_3;

    set @sort_stmts = (select GROUP_CONCAT(concat('`',attr_name,'`', ' ', `order`)) as 'tmp' from md_sort where table_name = tbl_n);
    set @sort_table = concat('create table temp_clean_table_3 select * from temp_clean_table_2 order by ', @sort_stmts);
    prepare stmt from @sort_table;
    execute stmt;

    -- drop orignial dirty table
    set @original_table_name = tbl_n;
    set @drop_original_table = concat('drop table ', @original_table_name);
    prepare stmt from @drop_original_table;
    execute stmt;

    -- create the clean table
    set @create_clean_table = concat('create table ', @original_table_name, ' select * from temp_clean_table_3');
    prepare stmt from @create_clean_table;
    execute stmt;

    drop table if exists temp_md_table;
    drop table if exists temp_clean_table_1;
    drop table if exists temp_clean_table_2;
    drop table if exists temp_clean_table_3;

end //

delimiter ;
