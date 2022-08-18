get_datas = """
select * from `digital_twin`.{}
where id=:id
"""

get_ids = """
select `id` from `digital_twin`.{}
"""

create_table = """
create table `digital_twin`.`{}`(
    `id` int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `date` date NOT NULL,
    `time` time NOT NULL,
    `pv_w` BIGINT,
    `pv_wh` BIGINT,
    `date_time` DATETIME,
     UNIQUE INDEX `datetime_UNIQUE` (`date` ASC, `time` ASC) VISIBLE
);
"""

insert_other_data = """
insert into `digital_twin`.`{}`(`date`,`time`,`pv_w`,`pv_wh`,`date_time`)
values(:date,:time,:pv_w,:pv_wh,:date_time);
"""

insert_battery_data = """
insert into `digital_twin`.`battery`(
`measurement_time`,
`FCAS_Event`,
`full_charge_energy`,
`nominal_energy`,
`expected_energy`,
`charge_p_max`,
`discharge_p_max`,
`available_blocks`,
`_3_phase_voltage`,
`_3_phase_current`,
`_3_phase_power`,
`_3_phase_reactive_power`,
`_3_phase_apparent_power`,
`power_factor`,
`frequency`,
`real_energy_imported`,
`real_energy_exported`,
`reactive_energy_imported`,
`reactive_energy_exported`,
`apparent_energy`,
`energy_price`,
`raise_6_sec_price`,
`raise_60_sec_price`,
`raise_5_min_price`,
`date_time`)
values(
:measurement_time,
:FCAS_Event,
:full_charge_energy,
:nominal_energy,
:expected_energy,
:charge_p_max,
:discharge_p_max,
:available_blocks,
:_3_phase_voltage,
:_3_phase_current,
:_3_phase_power,
:_3_phase_reactive_power,
:_3_phase_apparent_power,
:power_factor,
:frequency,
:real_energy_imported,
:real_energy_exported,
:reactive_energy_imported,
:reactive_energy_exported,
:apparent_energy,
:energy_price,
:raise_6_sec_price,
:raise_60_sec_price,
:raise_5_min_price,
:date_time);
"""
get_datas_order = """
select * from `digital_twin`.{}
order by {} {},date_time desc
"""