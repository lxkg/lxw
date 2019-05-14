#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "sqlite3-driver.h"
#include <time.h>
#include <math.h>

#define UPDATE_DEVICE_TYPE_2 "update global_info_tables set deviceindex ='%d',total_count ='%d',router_count='%d' where moteindex ='%d'"
#define UPDATE_DEVICE_TYPE_3 "update global_info_tables set deviceindex ='%d',total_count ='%d', relay_count='%d' where moteindex ='%d'"
#define UPDATE_DEVICE_TYPE_4 "update global_info_tables set deviceindex ='%d',total_count ='%d', sensor_count='%d' where moteindex ='%d'"
#define UPDATE_DEVICE_TYPE_5 "update global_info_tables set deviceindex ='%d',total_count ='%d', smartcontroller_count='%d' where moteindex ='%d'"

#define DEVICE_TABLE  "device_tables"
#define M2M_TABLE     "m2m_tables"
#define GLOBAL_TABLE  "global_info_tables"
#define USER_TABLE    "user_data_tables"

#define CREATE_GLOBAL_TABLES_STR     "create table if not exists "GLOBAL_TABLE" (moteindex integer primary key,\
		 total_count integer not null,router_count integer not null,relay_count integer not null,sensor_count integer not null,smartcontroller_count integer not null,\
			deviceindex integer not null,m2mindex integer not null,userdataindex integer not null)"\

#define CREATE_M2M_TABLES_STR        "create table if not exists "M2M_TABLE"( m2mindex integer primary key,M2M_handler_ID integer not null,start_mote_addr text not null,\
		 end_mote_addr text not null,start_mote_ID integer not null,end_mote_ID integer not null,originate_sensor_ID integer not null,\
			 act_sensor_ID integer not null,operation_symbol CHAR null,trigger_val text not null,trigger_logic text not null)"\
			 
#define CREATE_DEVICE_AND_TABLES_STR    "create table if not exists "DEVICE_TABLE"(deviceindex integer not null,deviceID integer primary key,\
		 role integer not null,capability integer not null,status integer not null,label integer not null,serial_num text not null);\
		 create table if not exists "USER_TABLE"(userdataindex integer not null,src_device_ID integer not null,\
		 data_type integer not null,data text not null,timestamp text not null)"\

#define SQL_MAX        512

const static  uint8_t mote_index = 1;
static struct gateway_sqlite_count  sqlite_mote_count; 
int mote_count_tables_init();
int sqlitestr_to_mac(char* src_str, linkaddr_t* dest_mac);
int str_to_buf(uint8_t str_operation,char* src_str, uint16_t* dstbuf,uint8_t *dstbuff);
static sqlite3* gateway_db=NULL;
/*********************************************sqlite3 init*************************************************************************************/
void  sqlite3_init(void)
{
	char *errmsg;
	char sql[SQL_MAX];
	memset(&sqlite_mote_count,0,sizeof(struct gateway_sqlite_count));
    if(sqlite3_open("holliot.gateway_db",&gateway_db) != SQLITE_OK)
    {
            printf("errmsg: %s\n",errmsg);
			return;
    }
	sprintf(sql,CREATE_DEVICE_AND_TABLES_STR);
	if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	 {
		    printf("create m2m table errmsg:%s\n",errmsg);
			return;
	 }
	 memset(sql,0x00,sizeof(sql));
	 sprintf(sql,CREATE_M2M_TABLES_STR);
	 if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	 {
		    printf("create m2m table errmsg:%s\n",errmsg);
			return;
	 }
	 memset(sql,0x00,sizeof(sql));
	 sprintf(sql,CREATE_GLOBAL_TABLES_STR);
	 if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	 {
		    printf("errmsg:%s\n",errmsg);
			return;
	 }	
     int ret = select_mote_count_tables(&sqlite_mote_count);
     if(ret == -1)
		 mote_count_tables_init(); 
}
/**************************************************delete sqlite3*********************************************************************************/
int delete_sqlite3(uint8_t delete_approach)
{
	char sql[SQL_MAX];
	char *errmsg;
	switch(delete_approach)
	{
		case DELETE_ALL_TABLE:
			sprintf(sql, "drop table  if exists "DEVICE_TABLE";drop table  if exists "M2M_TABLE";\
			drop table if exists "GLOBAL_TABLE";drop table if exists "USER_TABLE"");
			break;
		case DELETE_DEVICE_TABLE:
			sprintf(sql, "drop table  if exists "DEVICE_TABLE" ");
			break;
		case DELETE_M2M_TABLE:
			sprintf(sql, "drop table if exists "M2M_TABLE"");
			break;
		case DELETE_GLOBAL_TABLE:
			sprintf(sql, "drop table if exists "GLOBAL_TABLE"");
			break;
		case DELETE_USER_TABLE:
			sprintf(sql, "drop table if exists "USER_TABLE"");
			break;
		default: 
		     return 1;
	}
	if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	{
		    printf("drop tables errmsg:%s\n",errmsg);
	        return -1;
	}
    return 0;	
}
/****************************************************insert data to device table ***********************************************************************************/

int insert_device_table(uint8_t my_device_ID,struct device_manager* device)
{
	char sql[SQL_MAX]={'\0'};
	char *errmsg;
	char mac_addr[16]={0x00};
	int termpature_count;
	sprintf(mac_addr,"00.00.%x.%x",device->serial_num.u8[0],device->serial_num.u8[1]);
	sprintf(sql,"insert into "DEVICE_TABLE" values('%d',%d,'%d','%d','%d','%d','%s');",sqlite_mote_count.device_index++,
	my_device_ID,device->type,device->capability,device->status,device->label,mac_addr);
	switch(device->type)
	{
	    case 2: 
		        strncpy(sql+strlen(sql),UPDATE_DEVICE_TYPE_2,strlen(UPDATE_DEVICE_TYPE_2));
				termpature_count = ++sqlite_mote_count.router_count;
				break;
		case 3:
		        strncpy(sql+strlen(sql),UPDATE_DEVICE_TYPE_3,strlen(UPDATE_DEVICE_TYPE_3));
				termpature_count = ++sqlite_mote_count.relay_count;
				break;
		case 4:
		        strncpy(sql+strlen(sql),UPDATE_DEVICE_TYPE_4,strlen(UPDATE_DEVICE_TYPE_4));
				termpature_count = ++sqlite_mote_count.sensor_count;
				break;
		case 5:
		        strncpy(sql+strlen(sql),UPDATE_DEVICE_TYPE_5,strlen(UPDATE_DEVICE_TYPE_5));
				termpature_count = ++sqlite_mote_count.smartcontroller_count;
				break;
		default:
		       return -1;
	}
	sqlite_mote_count.total_count++;
    sprintf(sql,sql,sqlite_mote_count.device_index,sqlite_mote_count.total_count,termpature_count,mote_index);
    if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	{
		     printf("update DEVICE_TABLE fail:%s\n",errmsg);
			 return 1;   
	}	
	return 0;
}

/*******************************************************delete device tables data**********************************************************************/
int delete_device_table(uint8_t delete_approach, void* delete_data)
{
	char *errmsg;
	char sql[SQL_MAX];
	if(delete_approach == DELETE_BY_INDEX)
	{
         int index = *((int*)delete_data);
	     sprintf(sql,"delete from "DEVICE_TABLE"  where deviceindex ='%d' ",index);
	}else if(delete_approach == DELETE_BY_DEVICEID){
		 int device_ID =  *((int*)delete_data);
	     sprintf(sql,"delete from "DEVICE_TABLE"  where deviceID ='%d' ",device_ID);
	}else
		return -1;
	if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	{
		     printf("delete device_tables fail:%s\n",errmsg);
			 return 1;
	} 	
	return 0;
}
/******************************************************select device table info***************************************************************************/
int select_device_table(uint8_t select_approach, void* select_data, struct device_manager* device)
{
	int ncolumn,nrow;
	char *errmsg;
    char **resultp;
	char sql[SQL_MAX]={'\0'};
	char src_myaddr[16]={'\0'};
	switch(select_approach)
	{
		case SELECT_BY_INDEX:
			{
			  int index = *((int*)select_data);
			  sprintf(sql, "select * from "DEVICE_TABLE" where deviceindex='%d' ",index);
			  break;
			}
		case SELECT_BY_DEVICE_ID:
			{
			  int device_id = *((int*)select_data);
			  sprintf(sql, "select * from "DEVICE_TABLE" where deviceId='%d' ", device_id);
			  break;
			}
		case SELECT_BY_SERIAL_NUM:
			sprintf(sql, "select * from "DEVICE_TABLE" where serial_num='00.00.%s' ",(char*)select_data);
			break;
		case SELECT_BY_LABEL:
			{
			  int label = *((int*)select_data);
			  sprintf(sql, "select * from "DEVICE_TABLE" where label='%d' ",label);
			  break;
			}
	}
    if(sqlite3_get_table(gateway_db,sql,&resultp,&nrow,&ncolumn,&errmsg)==SQLITE_OK)
	{   
        if(ncolumn !=0)
	    {
				     device->type = atoi(strcpy(&device->type,resultp[ncolumn+2]));
				     device->capability = atoi(strcpy(&device->capability,resultp[ncolumn+3]));
				     device->status = atoi(strcpy(&device->status,resultp[ncolumn+4]));
				     device->label = atoi(strcpy((char*)&device->label,resultp[ncolumn+5]));
					 sqlitestr_to_mac(strcpy(src_myaddr,resultp[ncolumn+6]), &device->serial_num);
		}else
	    {
		    return -1;
		}
	}
    return 0;	
}
/*************************************************************************************************************************/


/************************************************m2m info insert or delete*******************************************************/
#define ADD_STR "update global_info_tables set m2mindex ='%d' where moteindex ='%d'"
int m2m_insert_or_delete_table(uint8_t m2m_operation,void*m2m_data)
{
	char startmac_addr[16]={'\0'};
	char endmac_addr[16]={'\0'};
	char m2m_logic[48]={'\0'};
	char sql[SQL_MAX];
	char *errmsg;
	if(m2m_operation == M2M_INSERT_OPERATION)
	{
		 struct M2M_info m2m_manger =*((struct M2M_info*)m2m_data);
		 sprintf(startmac_addr,"00.00.%x.%x",m2m_manger.start_mote_addr.u8[0],m2m_manger.start_mote_addr.u8[1]);
		 sprintf(endmac_addr,"00.00.%x.%x",m2m_manger.end_mote_addr.u8[0],m2m_manger.end_mote_addr.u8[1]);
	     sprintf(m2m_logic,"%x-%x-%x-%x-%x-%x-%x-%x-",m2m_manger.trigger_logic[0],m2m_manger.trigger_logic[1],
	     m2m_manger.trigger_logic[2],m2m_manger.trigger_logic[3],m2m_manger.trigger_logic[4],m2m_manger.trigger_logic[5],m2m_manger.trigger_logic[6],m2m_manger.trigger_logic[7]);
		 sprintf(sql,"insert into "M2M_TABLE" values('%d','%d','%s','%s','%d','%d','%d','%d','%c','%hd','%s');",sqlite_mote_count.m2m_index,m2m_manger.M2M_handler_ID,startmac_addr,endmac_addr,m2m_manger.start_mote_ID,m2m_manger.end_mote_ID,
		 m2m_manger.originate_sensor_ID,m2m_manger.act_sensor_ID,m2m_manger.operation_symbol,m2m_manger.trigger_val,m2m_logic);
		 sprintf(sql+strlen(sql),ADD_STR,++sqlite_mote_count.m2m_index,mote_index);
	}else if(m2m_operation == M2M_DELETE_OPERATION)
	{
		 int m2m_handID = *((int*)m2m_data);
		 sprintf(sql,"delete from "M2M_TABLE"  where  M2M_handler_ID='%d' ",m2m_handID); 	
	}else
		return -1;
	if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	{
		     printf("insert to m2m_table fail:%s\n",errmsg);
			 return 1;
	}  
	return 0;
}
/******************************************m2m update***************************************************/
int m2m_update_table(uint16_t m2m_handID,char operation_symbol,short trigger_val)
{
	 char sql[SQL_MAX];
	 char *errmsg;
	 sprintf(sql,"update m2m_tables set operation_symbol='%c' ,trigger_val='%hd'  where M2M_handler_ID ='%d'",
	 operation_symbol,operation_symbol,m2m_handID);
	 if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	 {
		   printf("update m2m_tables fail:%s\n",errmsg);
		   return 1;
	 }  
	 return 0;
}
/*************************************select m2m info****************************************************************/
int select_m2m_table(uint8_t select_approach, void* select_data, struct M2M_info *m2m_manger)
{
	int ncolumn,nrow,j;
	char *errmsg;
    char **resultp;
	char sql[SQL_MAX];
	char startmac_addr[16]={'\0'};
	char endmac_addr[16]={'\0'};
	char m2m_logic[48]={'\0'};
	if(select_approach == M2M_SELECT_INDEX)
	{
		 int index = *((int*)select_data);
		 sprintf(sql,"select *from "M2M_TABLE" where m2mindex='%d'",index);
	}else if(select_approach == M2M_SELECT_M2M_HANDER_ID)
	{
		 int m2m_handID = *((int*)select_data);
		 sprintf(sql,"select *from "M2M_TABLE" where M2M_handler_ID='%d'",m2m_handID);
	}else
		return -1;
	if(sqlite3_get_table(gateway_db,sql,&resultp,&nrow,&ncolumn,&errmsg)==SQLITE_OK)
    {    
		if(ncolumn != 0)
		{
				     m2m_manger->M2M_handler_ID = atoi(strcpy((char*)&m2m_manger->M2M_handler_ID,resultp[ncolumn+1]));
					 sqlitestr_to_mac(strcpy(startmac_addr,resultp[ncolumn+2]),&m2m_manger->start_mote_addr);
					 sqlitestr_to_mac(strcpy(endmac_addr,resultp[ncolumn+3]),&m2m_manger->end_mote_addr);
				     m2m_manger->start_mote_ID = atoi(strcpy((char*)&m2m_manger->start_mote_ID,resultp[ncolumn+4]));
				     m2m_manger->end_mote_ID = atoi(strcpy((char*)&m2m_manger->end_mote_ID,resultp[ncolumn+5]));
				     m2m_manger->originate_sensor_ID = atoi(strcpy((char*)&m2m_manger->originate_sensor_ID,resultp[ncolumn+6]));
					 m2m_manger->act_sensor_ID = atoi(strcpy((char*)&m2m_manger->act_sensor_ID,resultp[ncolumn+7]));
					 strcpy(&m2m_manger->operation_symbol,resultp[ncolumn+8]);
					 m2m_manger->trigger_val = atoi(strcpy((char*)&m2m_manger->trigger_val,resultp[ncolumn+9]));
					 str_to_buf(STR_TO_U16,strcpy(m2m_logic,resultp[ncolumn+10]),m2m_manger->trigger_logic,NULL);
		}else
		{
				 return -1;
		}
	}
	return 0;
}
/************************************************************************************************************************/


/*********************************************mote count table init*****************************************************************/
 int mote_count_tables_init()
{
	char sql[SQL_MAX];
	char *errmsg;
	sprintf(sql,"insert into "GLOBAL_TABLE" values('%d','%d','%d','%d','%d','%d','%d','%d','%d')",mote_index,0,0,0,0,0,0,0,0);
	if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	{
		     printf("update global_info_table fail:%s\n",errmsg);
			 return 1;
	} 
	return 0;	
}
/*******************************************delete mote count info*******************************************************************************************/
int delete_mote_count_tables()
{
	char sql[SQL_MAX];
	char *errmsg;
	sprintf(sql, "select * from "DEVICE_TABLE" where moteindex='%d' ",mote_index);  
	if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	{
		     printf("update global_info_table fail:%s\n",errmsg);
			 return 1;
	} 
	return 0;
}
/******************************************update mote count tables****************************************************************/
int update_mote_count_tables(uint8_t update_approach, int update_count)
{
	char sql[SQL_MAX];
	char *errmsg;
	switch(update_approach)
	{
		case UPDATE_TOTAL_COUNT:
			sprintf(sql, "update "GLOBAL_TABLE" set total_count='%d'  where moteindex ='%d'",update_count,mote_index);
			break;
		case UPDATE_ROUTER_COUNT:
			sprintf(sql,  "update "GLOBAL_TABLE" set router_count='%d'  where moteindex ='%d'",update_count,mote_index);
			break;
		case UPDATE_RELAY_COUNT:
			sprintf(sql,  "update "GLOBAL_TABLE" set relay_count='%d'  where moteindex ='%d'",update_count,mote_index);
			break;
	   case UPDATE_SENSOR_COUNT:
			sprintf(sql,  "update "GLOBAL_TABLE" set sensor_count='%d'  where moteindex ='%d'",update_count,mote_index);
			break;
		case UPDATE_SMARTCONTROLLER_COUNT:
			sprintf(sql,  "update "GLOBAL_TABLE" set smartcontroller_count='%d'  where moteindex ='%d'",update_count,mote_index);
			break;
		default:
		   return -1;
	}
	if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	{
		     printf("update global_info_table fail:%s\n",errmsg);
			 return 1;
	} 
	return 0;	
} 
/****************************************************select mote count table info*******************************************************************/
int select_mote_count_tables(struct gateway_sqlite_count *sqlite_message)
{
	int ncolumn,nrow;
	char *errmsg;
    char **resultp;
	char sql[SQL_MAX];
	sprintf(sql,"select *from "GLOBAL_TABLE" where moteindex='%d'",mote_index);
	if(sqlite3_get_table(gateway_db,sql,&resultp,&nrow,&ncolumn,&errmsg)==SQLITE_OK)
	{
		if(ncolumn != 0)
	    {
					 sqlite_message->total_count = atoi(strcpy((char*)&sqlite_message->total_count,resultp[ncolumn+1]));
				     sqlite_message->router_count = atoi(strcpy((char*)&sqlite_message->router_count,resultp[ncolumn+2]));
				     sqlite_message->relay_count = atoi(strcpy((char*)&sqlite_message->relay_count,resultp[ncolumn+3]));
				     sqlite_message->sensor_count = atoi(strcpy((char*)&sqlite_message->sensor_count,resultp[ncolumn+4]));
				     sqlite_message->smartcontroller_count = atoi(strcpy((char*)&sqlite_message->smartcontroller_count,resultp[ncolumn+5]));
				     sqlite_message->device_index = atoi(strcpy((char*)&sqlite_message->device_index,resultp[ncolumn+6]));
				     sqlite_message->m2m_index = atoi(strcpy((char*)&sqlite_message->m2m_index,resultp[ncolumn+7]));
				     sqlite_message->user_data_index = atoi(strcpy((char*)&sqlite_message->user_data_index,resultp[ncolumn+8]));
	    }else
	    {
					 return -1;
		}  
    }
	return 0;
}

/*******************************************************insert or delete user data tables*******************************************************************************/

int user_data_insert_delete_update_operation(uint8_t user_operation,void*user_datas)
{
	char sql[SQL_MAX];
	char *errmsg;
	char userdata[16]={'\0'};
	if(user_operation == USER_DATA_TABLE_INSERT )
	{	 
        struct user_data_t *user_data =(struct user_data_t*)user_datas;
		sprintf(userdata,"%x-%x-%x-%x-",user_data->raw_data[0],user_data->raw_data[1],user_data->raw_data[2],user_data->raw_data[3]);
        if(sqlite_mote_count.user_data_index<=USER_COUNT)
		   sprintf(sql,"insert into "USER_TABLE" values('%d','%d','%d','%s','%s');\
		   update "GLOBAL_TABLE" set userdataindex ='%d' where moteindex ='%d'",sqlite_mote_count.user_data_index-1,user_data->src_device_ID,user_data->data_type,userdata,user_data->timestamp,++sqlite_mote_count.user_data_index,mote_index);
        else
			sprintf(sql,"update "USER_TABLE" set data='%s',timestamp='%s' where timestamp=(select min(timestamp)\
			from "USER_TABLE" where data_type='%d'and src_device_ID='%d')",userdata,user_data->timestamp ,user_data->data_type,user_data->src_device_ID);
	}
	else if(user_operation == USER_DATA_TABLE_DELETE )
	{
		   int index = *((int*)user_datas);
		   sprintf(sql,"delete from "USER_TABLE"  where userdataindex='%d'",index);
	}	
	else if(user_operation == USER_DATA_TABLE_UPDATA )
	{
		  struct user_data_t *user_data =(struct user_data_t*)user_datas;
	}
	 if(sqlite3_exec(gateway_db,sql,NULL,NULL,&errmsg)!=SQLITE_OK)
	 {
		  printf("update "USER_TABLE" fail:%s\n",errmsg);
		  return 1;
	 }  	
}
/****************************************************************select user data info********************************************************************************/
int select_user_data(struct user_data_t *user_data,int index)
{
	char sql[SQL_MAX];
	char *errmsg;
	int ncolumn,nrow,i;
    char **resultp;
	sprintf(sql,"select *from "USER_TABLE" where userdataindex='%d'",index);
	if(sqlite3_get_table(gateway_db,sql,&resultp,&nrow,&ncolumn,&errmsg)==SQLITE_OK)
	{    
		   if(ncolumn != 0)
		   {
				 user_data->src_device_ID = atoi(strcpy((char*)&user_data->data_type,resultp[ncolumn+1]));
				 user_data->data_type = atoi(strcpy((char*)&user_data->src_device_ID,resultp[ncolumn+2]));
				 str_to_buf(STR_TO_U8,resultp[ncolumn+3],NULL,user_data->raw_data);
				 strcpy(user_data->timestamp,resultp[ncolumn+4]);
			}else
			{
					 return -1;
			}
    }
	return 0;
}


 int sqlitestr_to_mac(char* src_str, linkaddr_t* dest_mac)
{
	uint8_t int_arr[5];
	uint8_t i;
	if(strlen(src_str) > 14)
		return 0;
	for(i = 6; i < 11; i++)
	{
		if( src_str[i] >= '0' && src_str[i] <= '9')
		{
			int_arr[i-6] = src_str[i] - '0';
		}
		else if( src_str[i] >= 'A' && src_str[i] <= 'F'  )
		{
			int_arr[i-6] = src_str[i] - 55;
		}
		else if(i == 8 && src_str[i] == '.')
		{
			continue;
		}
		else
		{
			dest_mac->u8[0] = 0;
			dest_mac->u8[1] = 0;
			return 0;
		}
	}
	dest_mac->u8[0] = int_arr[0]*16 + int_arr[1];
	dest_mac->u8[1] = int_arr[3]*16 + int_arr[4];
	return 1;
}
/***************************************字符串转化成数组**********************************************/
 int str_to_buf(uint8_t str_operation,char* src_str, uint16_t* dstbuf,uint8_t* dstbuff)
{
	uint8_t int_arr[48];
	uint8_t i;
    int m=0,j=0;
	int len = strlen(src_str);
	for(i=0;i < len;i++)
	{
		if( src_str[i] >= '0' && src_str[i] <= '9')
		{
			int_arr[i] = src_str[i] - '0';
			j++;
		}
		else if( src_str[i] >= 'A' && src_str[i] <= 'F'  )
		{
			int_arr[i] = src_str[i] - 55;
			j++;
		}
		else if((src_str[i] == '-')&&(str_operation == STR_TO_U16))
		{
			 while(j>0)
			 {
				 dstbuf[m] += int_arr[i-j]*pow(16,j-1);
				 j--;
			 }
			 m++;
	    }else if((src_str[i] == '-')&&(str_operation == STR_TO_U8))
		{
			 while(j>0)
			 {
				dstbuff[m] += int_arr[i-j]*pow(16,j-1);
				j--;
			 }
			 m++;
	    }
	}
	return 1;
}




















