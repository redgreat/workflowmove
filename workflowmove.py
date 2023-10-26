#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @generate at 2023/10/26 08:51

import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime


# 创建数据库连接
con_src = pymysql.connect(
    host="pc-bp1zip05gl1b1ga3veo.rwlb.rds.aliyuncs.com",
    port=3306,
    database="serviceordercenter",
    user="user_service",
    password="Lunz2017",
    charset="utf8",
    cursorclass=DictCursor,
)

con_his = pymysql.connect(
    host="pc-bp1zip05gl1b1ga3veo.rwlb.rds.aliyuncs.com",
    port=3306,
    database="serviceordercenterhis",
    user="user_service",
    password="Lunz2017",
    charset="utf8",
    cursorclass=DictCursor,
)

# 写入2023 需要迁移单据
# CREATE TABLE serviceordercenterhis.move_workorder2023(
#     WorkOrderId CHAR(12) PRIMARY KEY,
#     BatchNo int,
#     IsConsume TINYINT DEFAULT 0 NOT NULL,
#     KEY `NON-BatchNo`(`BatchNo`)
# );
# INSERT INTO serviceordercenterhis.move_workorder2023(WorkOrderId, BatchNo)
# SELECT Id,ntile(10) OVER (ORDER BY Id) AS BatchNo
# FROM serviceordercenter.tb_workorderinfo a
# WHERE a.CreatedAt<'2023-01-01'
#   AND a.WorkStatus NOT IN (9);

# 还需要考虑： 1.如果his库已经有此主键的单据，是让其报错还是跳出？ 2.开启多线程，是单个Batch多线程还是不同Batch多线程？
# 3.开启多线程，需要将每单迁移逻辑调整为可调用函数

# 可复用SQL语句
# 取数据SQL
sql_move = (
    "SELECT WorkOrderId FROM move_workorder2023 WHERE BatchNo=%s AND IsConsume=0;"
)

# 更新临时表状态
sql_up = "UPDATE move_workorder2023 SET IsConsume = 1 WHERE WorkOrderId=%s;"

# 通过WorkOrderId 查找ItemId
sql_cu_item = (
    "SELECT Id FROM workflowruntimeitems WHERE TargetEntityId=%s AND Deleted=0 LIMIT 1;"
)
sql_co_item = "SELECT Id FROM workflowcompleteitems WHERE TargetEntityId=%s AND Deleted=0 LIMIT 1;"

# 通过ItemId 查找 StepId
sql_cu_step = (
    "SELECT Id FROM workflowruntimesteps WHERE RuntimeItemId=%s AND Deleted=0;"
)
sql_co_step = (
    "SELECT Id FROM workflowcompletesteps WHERE RuntimeItemId=%s AND Deleted=0;"
)

# 通过StepId取ActorId
sql_cu_actor = (
    "SELECT Id FROM workflowruntimeactors WHERE RuntimeStepId=%s AND Deleted=0;"
)
sql_co_actor = (
    "SELECT Id FROM workflowcompleteactors WHERE RuntimeStepId=%s AND Deleted=0;"
)

# 迁移his
sql_cu_ins1 = (
    "INSERT INTO workflowruntimeitems(Id, WorkflowItemId, TargetEntityId, CurrentStepId, Status, CreatedById, "
    "CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt) "
    "SELECT Id, WorkflowItemId, TargetEntityId, CurrentStepId, Status, CreatedById, CreatedAt, UpdatedById, "
    "UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowruntimeitems "
    "WHERE Id=%s;"
)

sql_cu_ins2 = (
    "INSERT INTO workflowruntimesteps(Id, RuntimeItemId, WorkflowNodeId, Name, SortOrder, StartedAt, DoneAt, "
    "ActorNumber, AcceptedNumber, DeclinedNumber, Status, CreatedById, CreatedAt, UpdatedById, UpdatedAt, "
    "Deleted, DeletedById, DeletedAt) "
    "SELECT Id, RuntimeItemId, WorkflowNodeId, Name, SortOrder, StartedAt, DoneAt, ActorNumber, AcceptedNumber, "
    "DeclinedNumber, Status, CreatedById, CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowruntimesteps "
    "WHERE Id=%s;"
)

sql_cu_ins3 = (
    "INSERT INTO workflowruntimeactivities(Id, RuntimeItemId, UserId, FullName, Status, Message, CreatedById, "
    "CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt, RuntimeStepId) "
    "SELECT Id, RuntimeItemId, UserId, FullName, Status, Message, CreatedById, CreatedAt, "
    "UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt, RuntimeStepId "
    "FROM serviceordercenter.workflowruntimeactivities "
    "WHERE RuntimeItemId=%s AND Deleted=0;"
)

sql_cu_ins4 = (
    "INSERT INTO workflowruntimerelatedactors(Id, RuntimeStepId, UserId, FullName, LoginName, Status, Notified, "
    "NotifiedAt, CreatedById, CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt) "
    "SELECT Id, RuntimeStepId, UserId, FullName, LoginName, Status, Notified, NotifiedAt, CreatedById, "
    "CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowruntimerelatedactors "
    "WHERE RuntimeStepId=%s AND Deleted=0;"
)

sql_cu_ins5 = (
    "INSERT INTO workflowruntimeactors(Id, RuntimeStepId, UserId, FullName, LoginName, Notified, NotifiedAt, "
    "Remark, Processed, ProcessedAt, WorkflowHandlingStatusId, Status, Active, IsAgent, AgentActorId, "
    "IsDelegate, DelegateActorId, Reason, OriginName, CreatedById, CreatedAt, UpdatedById, UpdatedAt, "
    "Deleted, DeletedById, DeletedAt) "
    "SELECT Id, RuntimeStepId, UserId, FullName, LoginName, Notified, NotifiedAt, Remark, Processed, "
    "ProcessedAt, WorkflowHandlingStatusId, Status, Active, IsAgent, AgentActorId, IsDelegate, DelegateActorId, "
    "Reason, OriginName, CreatedById, CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowruntimeactors "
    "WHERE Id=%s;"
)

sql_co_ins1 = (
    "INSERT INTO workflowcompleteitems(Id, WorkflowItemId, TargetEntityId, CurrentStepId, Status, CreatedById, "
    "CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt) "
    "SELECT Id, WorkflowItemId, TargetEntityId, CurrentStepId, Status, CreatedById, CreatedAt, UpdatedById, "
    "UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowcompleteitems "
    "WHERE Id=%s;"
)

sql_co_ins2 = (
    "INSERT INTO workflowcompletesteps(Id, RuntimeItemId, WorkflowNodeId, Name, SortOrder, StartedAt, DoneAt, "
    "ActorNumber, AcceptedNumber, DeclinedNumber, Status, CreatedById, CreatedAt, UpdatedById, UpdatedAt, "
    "Deleted, DeletedById, DeletedAt) "
    "SELECT Id, RuntimeItemId, WorkflowNodeId, Name, SortOrder, StartedAt, DoneAt, ActorNumber, AcceptedNumber, "
    "DeclinedNumber, Status, CreatedById, CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowcompletesteps "
    "WHERE Id=%s;"
)

sql_co_ins3 = (
    "INSERT INTO workflowcompleterelatedactors(Id, RuntimeStepId, UserId, FullName, LoginName, Status, Notified, "
    "NotifiedAt, CreatedById, CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt) "
    "SELECT Id, RuntimeStepId, UserId, FullName, LoginName, Status, Notified, NotifiedAt, CreatedById, "
    "CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowcompleterelatedactors "
    "WHERE RuntimeStepId=%s AND Deleted=0;"
)

sql_co_ins4 = (
    "INSERT INTO workflowcompleteactors(Id, RuntimeStepId, UserId, FullName, LoginName, Notified, NotifiedAt, "
    "Remark, Processed, ProcessedAt, WorkflowHandlingStatusId, Status, Active, IsAgent, AgentActorId, "
    "IsDelegate, DelegateActorId, Reason, OriginName, CreatedById, CreatedAt, UpdatedById, UpdatedAt, "
    "Deleted, DeletedById, DeletedAt) "
    "SELECT Id, RuntimeStepId, UserId, FullName, LoginName, Notified, NotifiedAt, Remark, Processed, "
    "ProcessedAt, WorkflowHandlingStatusId, Status, Active, IsAgent, AgentActorId, IsDelegate, DelegateActorId, "
    "Reason, OriginName, CreatedById, CreatedAt, UpdatedById, UpdatedAt, Deleted, DeletedById, DeletedAt "
    "FROM serviceordercenter.workflowcompleteactors "
    "WHERE Id=%s;"
)

# 清理源库
sql_cu_del1 = "DELETE FROM workflowruntimeitems WHERE Id=%s;"
sql_cu_del2 = "DELETE FROM workflowruntimesteps WHERE Id=%s;"
sql_cu_del3 = (
    "DELETE FROM workflowruntimeactivities WHERE RuntimeItemId=%s AND Deleted=0;"
)
sql_cu_del4 = (
    "DELETE FROM workflowruntimerelatedactors WHERE RuntimeStepId=%s AND Deleted=0;"
)
sql_cu_del5 = "DELETE FROM workflowruntimeactors WHERE Id=%s;"

sql_co_del1 = "DELETE FROM workflowcompleteitems WHERE Id=%s;"
sql_co_del2 = "DELETE FROM workflowcompletesteps WHERE Id=%s;"
sql_co_del3 = (
    "DELETE FROM workflowcompleterelatedactors WHERE RuntimeStepId=%s AND Deleted=0;"
)
sql_co_del4 = "DELETE FROM workflowcompleteactors WHERE Id=%s;"

# 取消外键约束
sql_nf = "SET FOREIGN_KEY_CHECKS = 0;"
# 开启外键约束
sql_hf = "SET FOREIGN_KEY_CHECKS = 1"

# 开始时间
start_time = datetime.now()

# 循环每批次单据，假设有10批次(NTILE数值)
for i in range(0, 10):
    cur_his = con_his.cursor()
    cur_his.execute(sql_move, i)
    res_ids = cur_his.fetchall()
    cur_his.close()

    # 先判断有没有取出值
    if res_ids:
        # 每批次单据较多，按行处理
        for res_id in res_ids:
            batch_start_time = datetime.now()
            order_id = res_id.get("WorkOrderId")
            # 开启游标,每个单开启一次,迁完就提交，可以尝试一个Batch开启一次
            cur_src = con_src.cursor()
            cur_his = con_his.cursor()
            try:
                # 关闭外键约束
                cur_src.execute(sql_nf)
                cur_his.execute(sql_nf)
                con_src.commit()
                con_his.commit()

                # 取ItemId,先找his的，his没有再找runtime的
                cur_src.execute(sql_co_item, [order_id])
                res_co_itemid = cur_src.fetchall()
                if res_co_itemid:
                    item_id = res_co_itemid[0].get("Id")
                    cur_his.execute(sql_co_ins1, [item_id])
                    cur_src.execute(sql_co_del1, [item_id])
                    # 取StepId
                    cur_src.execute(sql_co_step, [item_id])
                    res_co_stepids = cur_src.fetchall()
                    for res_co_stepid in res_co_stepids:
                        step_id = res_co_stepid.get("Id")
                        cur_his.execute(sql_co_ins2, [step_id])
                        cur_src.execute(sql_co_del2, [step_id])
                        cur_his.execute(sql_co_ins3, [step_id])
                        cur_src.execute(sql_co_del3, [step_id])
                        # 取Actor表数据
                        cur_src.execute(sql_co_actor, [step_id])
                        res_co_actorids = cur_src.fetchall()
                        for res_co_actorid in res_co_actorids:
                            actor_id = res_co_actorid.get("Id")
                            cur_his.execute(sql_co_ins4, [actor_id])
                            cur_src.execute(sql_co_del4, [actor_id])
                # 如果compoete表内无itemid
                else:
                    cur_src.execute(sql_cu_item, [order_id])
                    res_cu_itemid = cur_src.fetchall()
                    item_id = res_cu_itemid[0].get("Id")
                    cur_his.execute(sql_cu_ins1, [item_id])
                    cur_src.execute(sql_cu_del1, [item_id])
                    cur_his.execute(sql_cu_ins3, [item_id])
                    cur_src.execute(sql_cu_del3, [item_id])
                    # 取StepId
                    cur_src.execute(sql_cu_step, [item_id])
                    res_cu_stepids = cur_src.fetchall()
                    for res_cu_stepid in res_cu_stepids:
                        step_id = res_cu_stepid.get("Id")
                        cur_his.execute(sql_cu_ins2, [step_id])
                        cur_src.execute(sql_cu_del2, [step_id])
                        cur_his.execute(sql_cu_ins4, [step_id])
                        cur_src.execute(sql_cu_del4, [step_id])
                        # 取Actor表数据
                        cur_src.execute(sql_cu_actor, [step_id])
                        res_cu_actorids = cur_src.fetchall()
                        for res_cu_actorid in res_cu_actorids:
                            actor_id = res_cu_actorid.get("Id")
                            cur_his.execute(sql_cu_ins5, [actor_id])
                            cur_src.execute(sql_cu_del5, [actor_id])
                # 加回外键约束
                cur_src.execute(sql_hf)
                cur_his.execute(sql_hf)
                # 更新临时表状态
                cur_his.execute(sql_up, [order_id])
                # 一单迁移完后提交库数据
                con_his.commit()
                con_src.commit()
                batch_end_time = datetime.now()
                print(
                    f"单据 {order_id} 迁移耗时 {(batch_end_time - batch_start_time).total_seconds()} 秒！"
                )
            except Exception as e:
                print(e)
                con_his.rollback()
                con_src.rollback()
            finally:
                cur_his.close()
                cur_src.close()

end_time = datetime.now()
print(f"全部迁移完成，总计耗时 {(end_time - start_time).total_seconds()} 秒！")

con_src.close()
con_his.close()
