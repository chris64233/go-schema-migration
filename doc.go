// Package schemamigration 提供多租户数据库结构迁移的分阶段编排：
// 迁移计划发布、单租户执行的步骤领取/回执、兼容确认、暂停/恢复/回滚，
// 以及多租户分波次迁移（Batch）的冻结分波、波次闸门、失败率自动暂停、
// 失败租户重试/移出、审计与查询。服务自身无状态，并发安全与崩溃恢复
// 完全建立在 Store 的条件写（revision CAS）之上。
package schemamigration
