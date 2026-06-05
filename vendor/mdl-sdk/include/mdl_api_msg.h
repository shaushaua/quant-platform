/** 
 * 通联数据机密
 * -------------------------------------------------------------------
 * 通联数据股份公司版权所有 © 2013-2016
 *
 * 注意：本文所载所有信息均属于通联数据股份公司资产。本文所包含的知识和技术概念均属于
 * 通联数据产权，并可能由中国、美国和其他国家专利或申请中的专利所覆盖，并受商业秘密或
 * 版权法保护。
 * 除非事先获得通联数据股份公司书面许可，严禁传播文中信息或复制本材料。
 * 
 * DataYes CONFIDENTIAL
 * --------------------------------------------------------------------
 * Copyright © 2013-2016 DataYes, All Rights Reserved.
 * 
 * NOTICE: All information contained herein is the property of DataYes 
 * Incorporated. The intellectual and technical concepts contained herein are 
 * proprietary to DataYes Incorporated, and may be covered by China, U.S. and 
 * Other Countries Patents, patents in process, and are protected by trade 
 * secret or copyright law. 
 * Dissemination of this information or reproduction of this material is 
 * strictly forbidden unless prior written permission is obtained from DataYes.
 */

#pragma once

#include "mdl_api_types.h"

namespace datayes {
namespace mdl {
namespace mdl_api_msg {

static const uint16_t MDLVID_MDL_API = 101;

enum MDL_APIMessageID {
	MDLMID_MDL_API_ConnectingEvent = 1,
	MDLMID_MDL_API_ConnectErrorEvent = 2,
	MDLMID_MDL_API_DisconnectedEvent = 3,
	MDLMID_MDL_API_Timer = 4,
	MDLMID_MDL_API_MessageServiceTimeOutEvent = 5,
	MDLMID_MDL_API_MessageDiscardedEvent = 6
};

#pragma pack(1)

struct ConnectingEvent {
	enum {
		ServiceID = MDLSID_MDL_API,
		ServiceVer = MDLVID_MDL_API,
		MessageID = MDLMID_MDL_API_ConnectingEvent
	};
	MDLAnsiString Address;
};

struct ConnectErrorEvent {
	enum {
		ServiceID = MDLSID_MDL_API,
		ServiceVer = MDLVID_MDL_API,
		MessageID = MDLMID_MDL_API_ConnectErrorEvent
	};
	MDLAnsiString ErrorMessage;
	MDLAnsiString Address;
};

struct DisconnectedEvent {
	enum {
		ServiceID = MDLSID_MDL_API,
		ServiceVer = MDLVID_MDL_API,
		MessageID = MDLMID_MDL_API_DisconnectedEvent
	};
	MDLAnsiString ErrorMessage;
	MDLAnsiString Address;
};

struct TimerEvent {
	enum {
		ServiceID = MDLSID_MDL_API,
		ServiceVer = MDLVID_MDL_API,
		MessageID = MDLMID_MDL_API_Timer
	};
	int64_t Now;
};

struct MessageServiceTimeOutEvent {
	enum {
		ServiceID = MDLSID_MDL_API,
		ServiceVer = MDLVID_MDL_API,
		MessageID = MDLMID_MDL_API_MessageServiceTimeOutEvent
	};
	MDLAnsiString Address;
	uint32_t SvcID;
};


struct MessageDiscardedEvent {
	enum {
		ServiceID = MDLSID_MDL_API,
		ServiceVer = MDLVID_MDL_API,
		MessageID = MDLMID_MDL_API_MessageDiscardedEvent
	};
	MDLAnsiString Address;
	uint32_t SvcID;
	uint32_t MsgID;
};

#pragma pack()

} // namespace mdl_api_msg
} // namespace mdl
} // namespace datayes