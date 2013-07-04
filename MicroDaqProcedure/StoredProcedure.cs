using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections.Generic;


public class CLRProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void UpdateMeterValue(int ID, int Type, int State, int Quality, Double Value1, double Value2, double Value3, ref int ResultState, ref string RestultMessage)
    {
        Item item = new Item();
        Parameter parameter = new Parameter();

        item.ID = ID;
        item.Type = Type;
        item.State = State;
        item.Quality = Quality;
        item.Value1 = Value1;
        item.Value2 = Value2;
        item.Value3 = Value3;

        if (Type == 35)
        {
            item.Type = 32;
        }
        if ((Int32.Parse((SqlHelper.ExecuteScalar("SELECT COUNT(1) FROM dbo.ProcessItem Where Slave=" + ID).ToString()))) > 0)
        {
            item.exitInProcessItem = true;
        }
        parameter = GetParameter(item);

        //添加折算系数
        if (Type == 32 && item.exitInProcessItem == true)
        {
            object retValueRate = SqlHelper.ExecuteScalar("Select ValueRate From dbo.ProcessItem Where Slave=" + ID);
            if (retValueRate != null && retValueRate.ToString().Trim() != "")
            {
                float valueRate = float.Parse(retValueRate.ToString());
                item.Value1 = Value1 * valueRate;
                item.Value2 = Value2 * valueRate;
                item.Value3 = Value3 * valueRate;
            }
        }

        if (item.ID < 10000)
        {
            UpdateTransientData(item, parameter,ref ResultState,ref RestultMessage);



            if (item.Quality == 192)
            {
                UpdateRemoteControl(item, ref ResultState, ref RestultMessage);

                if (item.exitInProcessItem && (item.State == 1 || item.State == 4 || item.State == 8))
                {
                    UpdateHistoryData(item, parameter,ref ResultState,ref RestultMessage);
                }
            }
        }

    }

    /// <summary>
    /// 更新即时数据
    /// </summary>
    /// <param name="item"></param>
    public static void UpdateTransientData(Item item, Parameter parameter, ref int ResultState, ref string RestultMessage)
    {
        try
        {
            bool exitInItems_Value = false;
            if (Int32.Parse((SqlHelper.ExecuteScalar("SELECT COUNT(1) FROM dbo.Items_Value Where ID=" + item.ID).ToString())) > 0)
            {
                exitInItems_Value = true;
            }
            if (exitInItems_Value)
            {
                string update = @"Update Items_Value Set Type=" + item.Type + ",State=" + item.State + ",Quality=" + item.Quality + ","
                    + "Value1=" + item.Value1 + ",Value2=" + item.Value2 + ",Value3=" + item.Value3 + ",[Time]='" + DateTime.Now.ToString() + "'"
                    + " Where ID=" + item.ID;
                SqlHelper.ExecuteNonQuery(update);
            }
            else
            {
                string insert = @"Insert Into Items_Value (ID,Type,State,Quality,Value1,Value2,Value3,Time) Values("
                    + item.ID + "," + item.Type + "," + item.State + "," + item.Quality + "," +
                    +item.Value1 + "," + item.Value2 + "," + item.Value3 + ","
                    + "'" + DateTime.Now.ToString() + "'"
                    + ")";
                SqlHelper.ExecuteNonQuery(insert);
            }

            if (item.exitInProcessItem && (item.State == 1 || item.State == 8))
            {
                UpdateAlarm(item, parameter,ref ResultState,ref RestultMessage);
            }
        }
        catch (Exception ex)
        {
            ResultState += 1;
            RestultMessage += "UpdateTransientData:" + ex.Message+";";
        }

    }
    /// <summary>
    /// 更新受控设备状态
    /// </summary>
    public static void UpdateRemoteControl(Item item, ref int ResultState, ref string RestultMessage)
    {
        try
        {
            if ((int.Parse(SqlHelper.ExecuteScalar("Select Count(1) From RemoteControl WHERE slave =" + item.ID).ToString())) > 0)
            {
                SqlHelper.ExecuteNonQuery("Update RemoteControl Set remainSecond =" + (int.Parse(Math.Floor(item.Value1).ToString()) & 65535) + ",State=" + item.State + " Where Slave=" + item.ID + "");
            }
        }
        catch (Exception ex)
        {
            ResultState += 2;
            RestultMessage += "UpdateRemoteControl:" + ex.Message+";";
        }
    }
    /// <summary>
    /// 更新报警灯
    /// </summary>
    public static void UpdateAlarm(Item item, Parameter parameter, ref int ResultState, ref string RestultMessage)
    {
        try
        {
            object retLastAltet = SqlHelper.ExecuteScalar("SELECT alerttime FROM items_value WHERE id =" + item.ID);
            DateTime? latsAltet = null;
            if (retLastAltet != null && retLastAltet.ToString().Trim() != "")
            {
                latsAltet = DateTime.Parse(retLastAltet.ToString());
            }
            if (latsAltet == null || latsAltet.Value.AddSeconds(parameter.Rate) <= DateTime.Now)
            {
                List<string> listSql = new List<string>();
                //更新报警记录
                if (parameter.Logic.Trim() == "between")
                {
                    if (item.Value1 < parameter.YellowMin || item.Value1 > parameter.YellowMax)
                    {
                        string guid = Guid.NewGuid().ToString().Replace("-", "");
                        string insertAlter = @"INSERT INTO dbo.ProcessItemAlertRecord (id,processItemId,[timestamp],[value],[confirm]) VALUES("
                            + "'" + guid + "',"
                            + "'" + parameter.UUID + "',"
                            + "'" + DateTime.Now.ToString() + "',"
                            + item.Value1 + ",'未确定'" + ")";
                        string updateProcess = @"Update dbo.ProcessItem Set alertRecordId='" + guid + "' Where Slave=" + item.ID;
                        string updateItems = "Update items_value Set alertTime='" + DateTime.Now.ToString() + "' Where ID=" + item.ID;

                        listSql.Add(insertAlter);
                        listSql.Add(updateProcess);
                        listSql.Add(updateItems);
                    }
                }
                //计算报警灯状态
                string updateItemsAlter = "";
                if (parameter.Logic.Trim() == "between")
                {
                    if (item.Value1 > parameter.YellowMin && item.Value1 < parameter.YellowMax)
                    {
                        updateItemsAlter = "UPDATE items_value SET alert = 4 WHERE id = " + item.ID;
                    }
                    else
                    {
                        if (item.Value1 < parameter.RedMin || item.Value1 > parameter.RedMax)
                        {
                            updateItemsAlter = "UPDATE items_value SET alert = 1 WHERE id = " + item.ID;
                        }
                        else
                        {
                            updateItemsAlter = "UPDATE items_value SET alert = 2 WHERE id = " + item.ID;
                        }
                    }
                }
                else
                {
                    updateItemsAlter = "UPDATE items_value SET alert = 4 WHERE id = " + item.ID;
                }
                listSql.Add(updateItemsAlter);

                SqlHelper.ExecuteSqls(listSql);
            }
        }
        catch (Exception ex)
        {
            ResultState += 3;
            RestultMessage += "UpdateAlarm:"+ex.Message+";";
        }
    }
    /// <summary>
    /// 更新历史数据
    /// </summary>
    public static void UpdateHistoryData(Item item, Parameter parameter, ref int ResultState, ref string RestultMessage)
    {
        try
        {
            string nowTime = DateTime.Now.ToString();
            List<string> listSql = new List<string>();

            int retZZ = Int32.Parse(SqlHelper.ExecuteScalar("Select Count(1) From sysobjects Where name='zz" + parameter.UUID + "' and xtype='U'").ToString());
            if (retZZ > 0)
            {
                object retLastZZ = SqlHelper.ExecuteScalar("SELECT top 1 zztime FROM items_value WHERE id =" + item.ID);
                DateTime? latsZZ = null;
                if (retLastZZ != null && retLastZZ.ToString().Trim() != "")
                {
                    latsZZ = DateTime.Parse(retLastZZ.ToString());
                }
                if (latsZZ == null || latsZZ.Value.AddSeconds(parameter.Rate) <= DateTime.Now)
                {
                    #region 获取假数据 Value2
                    if (item.Value1 < parameter.YellowMin || item.Value1 > parameter.YellowMax)
                    {
                        float yellowMin = parameter.YellowMin;
                        if (item.Type == 32)
                        {
                            yellowMin = 0;
                        }
                        item.Value2 = float.Parse(((Math.Abs(yellowMin) + Math.Floor((new Random()).Next(1) * (parameter.YellowMax - yellowMin)))).ToString());
                        parameter.Warning = 1;
                    }
                    else
                    {
                        item.Value2 = item.Value1;
                        parameter.Warning = 0;
                    }
                    SqlHelper.ExecuteNonQuery("Update Items_Value Set Value2=" + item.Value2 + ",zzTime='" + DateTime.Now.ToString() + "' Where ID=" + item.ID);
                    #endregion

                    #region 更新ZZ表 和 35分钟数据表
                    listSql.Clear();
                    listSql.Add("Insert Into dbo.zz" + parameter.UUID + " (value,value2,timestamp,productionState,flag) Values(" + item.Value1 + "," + item.Value2 + ",'" + nowTime + "'," + parameter.ProductionState + "," + parameter.Warning + ")");
                    //zzHistory表中存35分钟内的数据，如过timestamp+35Minute>现在的时间,删除掉
                    listSql.Add("Delete From dbo.zzHistory Where timestamp<'" + (DateTime.Parse(nowTime)).AddMinutes(-35).ToString() + "'");
                    listSql.Add("INSERT INTO dbo.zzHistory( value ,value2 ,productionState , timestamp , flag , uuid) VALUES (" + item.Value1 + "," + item.Value2 + "," + parameter.ProductionState + ",'" + nowTime + "'," + parameter.Warning + ",'" + parameter.UUID + "')");
                    SqlHelper.ExecuteSqls(listSql);

                    #endregion

                    #region 更新ZZ汇总数据
                    if (item.Type == 32)
                    {
                        object UUIDM3 = null;

                        UUIDM3 = SqlHelper.ExecuteScalar("SELECT TOP 1 id FROM dbo.ProcessItem WHERE slave =" + (item.ID + 30000));

                        if (UUIDM3 != null && UUIDM3.ToString().Trim() != "")
                        {
                            float sumValue1 = 0, sumValue2 = 0;
                            using (SqlDataReader dr = SqlHelper.ExecuteReader("Select Sum(Value) as SV1,Sum(Value2) as SV2 From dbo.zzHistory Where UUID='" + parameter.UUID + "'"))
                            {
                                while (dr.Read())
                                {
                                    sumValue1 = dr["SV1"] == null ? 0 : float.Parse(dr["SV1"].ToString());
                                    sumValue2 = dr["SV2"] == null ? 0 : float.Parse(dr["SV2"].ToString());
                                }
                            }
                            Item itemM3 = new Item();
                            itemM3.ID = item.ID + 30000;
                            itemM3.Type = item.Type;
                            itemM3.State = item.State;
                            itemM3.Quality = 192;
                            itemM3.Value1 = sumValue1;
                            itemM3.Value2 = sumValue2;
                            itemM3.Value3 = 0;
                            itemM3.exitInProcessItem = true;
                            Parameter parameterM3 = GetParameter(itemM3);

                            UpdateTransientData(itemM3, parameterM3,ref ResultState,ref RestultMessage);

                            listSql.Clear();
                            listSql.Add("Insert Into dbo.zz" + UUIDM3.ToString() + " (value,value2,timestamp,productionState,flag) Values(" + sumValue1 + "," + sumValue2 + ",'" + nowTime.ToString() + "'," + parameter.ProductionState + "," + parameter.Warning + ")");
                            listSql.Add("Update Items_value Set zzTime='" + nowTime + "' Where ID=" + itemM3.ID);
                            SqlHelper.ExecuteSqls(listSql);
                        }
                    }
                    #endregion
                }
            }
        }
        catch (Exception ex)
        {
            ResultState += 4;
            RestultMessage += "UpdateHistoryData:"+ex.Message;
        }
    }
    /// <summary>
    /// 获取参数
    /// </summary>
    public static Parameter GetParameter(Item item)
    {
        Parameter parameter = new Parameter();
        if (item.exitInProcessItem)
        {
            string str = "Select id,minimum,maximum,yellowMin,yellowMax,updaterate,logic,productionState From dbo.ProcessItem Where Slave=" + item.ID;
            using (SqlDataReader dr = SqlHelper.ExecuteReader("Select id,minimum,maximum,yellowMin,yellowMax,updaterate,logic,productionState From dbo.ProcessItem Where Slave=" + item.ID))
            {
                while (dr.Read())
                {
                    parameter.UUID = dr["ID"].ToString();
                    parameter.YellowMin = dr["yellowMin"] == null ? 0 : float.Parse(dr["yellowMin"].ToString());
                    parameter.YellowMax = dr["yellowMax"] == null ? 0 : float.Parse(dr["yellowMax"].ToString());
                    parameter.RedMin = dr["minimum"] == null ? 0 : float.Parse(dr["minimum"].ToString());
                    parameter.RedMax = dr["maximum"] == null ? 0 : float.Parse(dr["maximum"].ToString());
                    parameter.Rate = dr["updaterate"] == null ? 60 : int.Parse(dr["updaterate"].ToString());
                    parameter.Logic = dr["logic"] == null ? "" : dr["logic"].ToString();
                    parameter.ProductionState = dr["productionState"] == null ? 0 : int.Parse(dr["productionState"].ToString());
                }

            }
        }
        return parameter;
    }
};

public partial class CLRTrigger
{
    [Microsoft.SqlServer.Server.SqlTrigger(Name = "Trigger_ItemsQualityRecord", Target = "dbo.Items_Value", Event = "FOR UPDATE")]
    public static void Trigger_ItemsQualityRecord()
    {
        if (SqlContext.TriggerContext.TriggerAction == TriggerAction.Update)
        {
            object typeN = 0, stateN = 0, qualityN = 0, alertN = 0;
            object typeO = 0, stateO = 0, qualityO = 0, alertO = 0, idO = 0;
            object value1O = 0, value2O = 0, value3O = 0;
            using (SqlDataReader drN = SqlHelper.ExecuteReader("Select * From Inserted"))
            {
                while (drN.Read())
                {

                    typeN = drN["type"]==DBNull.Value?null: drN["type"];
                    stateN = drN["state"] == DBNull.Value ? null : drN["state"];
                    qualityN = drN["quality"] == DBNull.Value ? null : drN["quality"];
                    alertN = drN["alert"] == DBNull.Value ? null : drN["alert"];
                }
            }
            using (SqlDataReader drO = SqlHelper.ExecuteReader("Select * from Deleted"))
            {
                while (drO.Read())
                {
                    typeO = drO["type"] == DBNull.Value ? null : drO["type"];
                    stateO = drO["state"] == DBNull.Value ? null : drO["state"];
                    qualityO = drO["quality"] == DBNull.Value ? null : drO["quality"];
                    alertO = drO["alert"] == DBNull.Value ? null : drO["alert"];

                    idO = drO["id"] == DBNull.Value ? null : drO["id"];
                    value1O = drO["value1"] == DBNull.Value ? null : drO["value1"];
                    value2O = drO["value2"] == DBNull.Value ? null : drO["value2"];
                    value3O = drO["value3"] == DBNull.Value ? null : drO["value3"];
                }
            }
           
            if (!(Equals(typeN,typeO)) || !(Equals(stateN,stateO)) || !(Equals(qualityN,qualityO)) || !(Equals(alertN,alertO)))
            {
                string insertSql = @"Insert Into dbo.ItemsQualityRecord (slave,time,type,state,quality,value1,value2,value3,alert) Values(@slave,@time,@type,@state,@quality,@value1,@value2,@value3,@alert)";
                SqlParameter[] pars = new SqlParameter[] { 
                  new SqlParameter("@slave",SqlDbType.Int),
                  new SqlParameter("@time",SqlDbType.DateTime),
                  new SqlParameter("@type",SqlDbType.Int),
                  new SqlParameter("@state",SqlDbType.Int),
                  new SqlParameter("@quality",SqlDbType.Int),
                  new SqlParameter("@value1",SqlDbType.Float),
                  new SqlParameter("@value2",SqlDbType.Float),
                  new SqlParameter("value3",SqlDbType.Float),
                  new SqlParameter("@alert",SqlDbType.Int)
                };

                pars[0].Value = idO;
                pars[1].Value = DateTime.Now;
                pars[2].Value = typeO;
                pars[3].Value = stateO;
                pars[4].Value = qualityO;
                pars[5].Value = value1O;
                pars[6].Value = value2O;
                pars[7].Value = value3O;
                pars[8].Value = alertO;

                SqlHelper.ExecuteNonQuery(insertSql, pars);
            }
        }
    }
}

public class SqlHelper
{
    private static void AttachParameters(SqlCommand command, SqlParameter[] commandParameters)
    {
        if (command == null) throw new ArgumentNullException("command");
        if (commandParameters != null)
        {
            foreach (SqlParameter p in commandParameters)
            {
                if (p != null)
                {
                    // 检查未分配值的输出参数,将其分配以DBNull.Value.
                    if ((p.Direction == ParameterDirection.InputOutput || p.Direction == ParameterDirection.Input) &&
                        (p.Value == null)||p.Value==DBNull.Value)
                    {
                        p.Value = DBNull.Value;
                    }
                    command.Parameters.Add(p);
                }
            }
        }
    }
  
   
    /// <summary>
    /// 执行T-Sql语句，返回受影响的行数
    /// </summary>
    /// <param name="cmdText"></param>
    /// <returns></returns>
    public static int ExecuteNonQuery(string cmdText)
    {
        return ExecuteNonQuery(cmdText, null);
    }

    public static int ExecuteNonQuery(string cmdText, SqlParameter[] pars)
    {
        SqlConnection connection = new SqlConnection("context connection=true");
        try
        {
            connection.Open();
            SqlCommand command = new SqlCommand(cmdText, connection);
            if (pars != null)
            {
                AttachParameters(command,pars);
            }
            return command.ExecuteNonQuery();
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            connection.Close();
        }
    }

    /// <summary>
    /// 执行查询，返回结果集的第一行第一列
    /// </summary>
    /// <param name="cmdText"></param>
    /// <returns></returns>
    public static object ExecuteScalar(string cmdText)
    {
        return ExecuteScalar(cmdText, null);
    }

    public static object ExecuteScalar(string cmdText, SqlParameter[] pars)
    {
        SqlConnection connection = new SqlConnection("context connection=true");
        try
        {
            connection.Open();
            SqlCommand command = new SqlCommand(cmdText, connection);
            if (pars != null)
            {
                AttachParameters(command, pars);
            }
            return command.ExecuteScalar();
        }
        catch (Exception e)
        {
            throw e;
        }
        finally
        {
            connection.Close();
        }
    }

    /// <summary>
    /// 执行Sql语句，返回SqlDataReader对象
    /// </summary>
    /// <param name="cmdText"></param>
    /// <returns></returns>
    public static SqlDataReader ExecuteReader(string cmdText)
    {
        return ExecuteReader(cmdText, null);
    }

    public static SqlDataReader ExecuteReader(string cmdText, SqlParameter[] pars)
    {
        SqlConnection connection = new SqlConnection("context connection=true");
        try
        {
            connection.Open();
            SqlCommand command = new SqlCommand(cmdText, connection);
            if (pars != null)
            {
                AttachParameters(command, pars);
            }
            return command.ExecuteReader(CommandBehavior.CloseConnection);
        }
        catch (Exception e)
        {
            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
            }
            throw e;
        }
        finally
        {
           // connection.Close();
        }
    }

    /// <summary>
    /// 执行Sql语句数组
    /// </summary>
    /// <param name="strs">Sql数组</param>
    /// <returns></returns>
    public static int ExecuteSqls(string[] strs)
    {
        int count = 0;
        SqlConnection connection = new SqlConnection("context connection=true");
        connection.Open();
        SqlTransaction tran = connection.BeginTransaction();
        SqlCommand command = new SqlCommand();
        command.Connection = connection;
        command.Transaction = tran;
        try
        {
            foreach (string s in strs)
            {
                command.CommandType = CommandType.Text;
                command.CommandText = s;
                count = count + command.ExecuteNonQuery();
            }
            tran.Commit();
        }
        catch (Exception ex)
        {
            tran.Rollback();
            count = 0;
            throw new ArgumentNullException(ex.Message);
        }
        finally
        {
            connection.Close();
        }
        return count;
    }
    /// <summary>
    /// 执行Sql语句数组
    /// </summary>
    /// <param name="listStrs"></param>
    /// <returns></returns>
    public static int ExecuteSqls(List<string> listStrs)
    {
        return ExecuteSqls(listStrs.ToArray());
    }
};

public class Parameter
{
    public string UUID;
    public float YellowMin;
    public float YellowMax;
    public float RedMin;
    public float RedMax;
    public int Rate;
    public string Logic;
    public int ProductionState;
    public int Warning;
};
public class Item
{
    public int ID, Type, State, Quality;
    public Double Value1, Value2, Value3;
    

    public bool exitInProcessItem = false; //是否存在此仪表
};




