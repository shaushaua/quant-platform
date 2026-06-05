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

#include "base/base.h"
#include "mdl_api_types.h"
#include "mdl_sys_msg.h"
#include "mdl_api_msg.h"
#include <vector>
#include <string.h>

namespace datayes {
namespace mdl {

///////////////////////////////////////////////////////////////////////////////////////////////////////////
class MDLMessage : public RefCounted {
public:
	DTAPIMETHOD(MDLMessageHead*) GetHead() const = 0;
	DTAPIMETHOD(char*) GetBody() const = 0;
	uint32_t GetBodySize() const;
	RefCountedPtrT<MDLMessage> Copy() const;

protected:
	DTAPIMETHOD(MDLMessage*) _Copy() const = 0;
};
typedef RefCountedPtrT<MDLMessage> MDLMessagePtr;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
class Subscriber : public RefCounted {
public:

	template <class T> 
	void SubcribeMessage() {
		AddSubscription(T::ServiceID, T::ServiceVer, T::MessageID);
	}

	template <class T> 
	void SubcribeMessageByFieldValues(const char* fieldName, const char** fieldValues, size_t fieldValuesCount) {
		AddSubscriptionByFieldValues(T::ServiceID, T::ServiceVer, T::MessageID, fieldName, fieldValues, fieldValuesCount);
	}

	template <class T> 
	void SubcribeMessageByUTF8FieldValues(const char* fieldName, const char** fieldValues, size_t fieldValuesCount) {
		std::vector<std::string> utf8StringValues;
		std::string utf8;
		for (size_t i = 0; i < fieldValuesCount; ++i) {
			size_t cansi = strlen(fieldValues[i]);
			utf8.resize(cansi, ' ');
			uint32_t c = DllConvertAnsiToUTF8(fieldValues[i], cansi, (char*)utf8.c_str(), (uint32_t)utf8.size());
			if (c > utf8.size()) {
				utf8.resize(c);
				c = DllConvertAnsiToUTF8(fieldValues[i], cansi, (char*)utf8.c_str(), (uint32_t)utf8.size());
			}
			utf8.resize(c);
			utf8StringValues.push_back(utf8);
		}
		std::vector<const char*> utf8CStrValues;
		for (size_t i = 0; i < utf8StringValues.size(); ++i) {
			utf8CStrValues.push_back(utf8StringValues[i].c_str());
		}
		AddSubscriptionByFieldValues(T::ServiceID, T::ServiceVer, T::MessageID, fieldName, utf8CStrValues.empty() ? 0 : &utf8CStrValues[0], utf8CStrValues.size());
	}

	template <class T> 
	void UnSubcribeMessage() {
		DelSubscription(T::ServiceID, T::ServiceVer, T::MessageID);
	}

	template <class T> 
	void UnSubcribeMessageByFieldValues(const char* fieldName, const char** fieldValues, size_t fieldValuesCount) {
		DelSubscriptionByFieldValues(T::ServiceID, T::ServiceVer, T::MessageID, fieldName, fieldValues, fieldValuesCount);
	}

	template <class T> 
	void UnSubcribeMessageByUTF8FieldValues(const char* fieldName, const char** fieldValues, size_t fieldValuesCount) {
		std::vector<std::string> utf8StringValues;
		std::string utf8;
		for (size_t i = 0; i < fieldValuesCount; ++i) {
			size_t cansi = strlen(fieldValues[i]);
			utf8.resize(cansi, ' ');
			uint32_t c = DllConvertAnsiToUTF8(fieldValues[i], cansi, (char*)utf8.c_str(), (uint32_t)utf8.size());
			if (c > utf8.size()) {
				utf8.resize(c);
				c = DllConvertAnsiToUTF8(fieldValues[i], cansi, (char*)utf8.c_str(), (uint32_t)utf8.size());
			}
			utf8.resize(c);
			utf8StringValues.push_back(utf8);
		}
		std::vector<const char*> utf8CStrValues;
		for (size_t i = 0; i < utf8StringValues.size(); ++i) {
			utf8CStrValues.push_back(utf8StringValues[i].c_str());
		}
		DelSubscriptionByFieldValues(T::ServiceID, T::ServiceVer, T::MessageID, fieldName, utf8CStrValues.empty() ? 0 : &utf8CStrValues[0], utf8CStrValues.size());
	}


	DTAPIMETHOD(void) AddSubscription(uint8_t svrID, uint16_t svrVersion, uint16_t msgID) = 0;
	DTAPIMETHOD(void) AddSubscriptionByFieldValues(uint8_t svrID, uint16_t svrVersion, uint16_t msgID, const char* fieldName, const char** fieldValues, uint32_t fieldValueCount) = 0;
	
	DTAPIMETHOD(void) DelSubscription(uint8_t svrID, uint16_t svrVersion, uint16_t msgID) = 0;
	DTAPIMETHOD(void) DelSubscriptionByFieldValues(uint8_t svrID, uint16_t svrVersion, uint16_t msgID, const char* fieldName, const char** fieldValues, uint32_t fieldValueCount) = 0;
	DTAPIMETHOD(void) ClearSubscriptions() = 0;

	DTAPIMETHOD(void) SetHeartbeatInterval(uint32_t interval) = 0;
	DTAPIMETHOD(uint32_t) GetHeartbeatInterval() = 0;

	DTAPIMETHOD(void) SetHeartbeatTimeout(uint32_t tmout) = 0;
	DTAPIMETHOD(uint32_t) GetHeartbeatTimeout() = 0;

	DTAPIMETHOD(void) SetUserName(const char* usrName) = 0;
	DTAPIMETHOD(const char*) GetUserName() = 0;

	DTAPIMETHOD(void) SetPassword(const char* passwd) = 0;
	DTAPIMETHOD(const char*) GetPassword() = 0;

	DTAPIMETHOD(void) SetMessageEncoding(MDLMessageEncoding encoding) = 0;	
	DTAPIMETHOD(MDLMessageEncoding) GetMessageEncoding() = 0;

	DTAPIMETHOD(void) SetServerAddress(const char* addr) = 0;	
	DTAPIMETHOD(const char*) GetServerAddress() = 0;

	DTAPIMETHOD(bool) PostRequest(MDLMessage* request) = 0;
	DTAPIMETHOD(bool) SendRequest(MDLMessage* request) = 0;

	DTAPIMETHOD(const char*) Connect() = 0;
    DTAPIMETHOD(void) SetReadBufferSize(int) = 0;
    DTAPIMETHOD(void) SetSendMacAuth(bool send) = 0;
    DTAPIMETHOD(void) EnableServerSelect(bool enable) = 0;

    DTAPIMETHOD(void) ReSubscribe() = 0;
    DTAPIMETHOD(const char*) GetSubscription() = 0;

	DTAPIMETHOD(void) EnableMergeMessage(bool enable) = 0;
};
typedef RefCountedPtrT<Subscriber> SubscriberPtr;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
enum PublisherType {
	PUBLISH_TYPE_UNDEFINED = 0,
	PUBLISH_TYPE_TCP = 1,
	PUBLISH_TYPE_REDIS = 2,
    PUBLISH_TYPE_WEBSOCKET = 3,
    PUBLISH_TYPE_MULTICAST = 4
};

class Publisher : public RefCounted {
public:
	DTAPIMETHOD(PublisherType) GetType() = 0;
	DTAPIMETHOD(const char*) Listen() = 0;
	DTAPIMETHOD(void) SetListenAddress(const char* usrName) = 0;
	DTAPIMETHOD(const char*) GetListenAddress() = 0;
};
typedef RefCountedPtrT<Publisher> PublisherPtr;

///////////////////////////////////////////////////////////////////////////////////////////////////////////
class MessageHandlerBase {
public:
	DTAPIMETHOD(void) OnMessage(Subscriber* sender, const MDLMessage* msg) = 0;
};

class AutoSubscriber : public RefCounted {
public:
    virtual ~AutoSubscriber() {}

    DTAPIMETHOD(void) SetURL(const char * url) = 0;
    DTAPIMETHOD(void) SetToken(const char * token) = 0;
    
    DTAPIMETHOD(void) SetOptionsJson(const char * options_json) = 0;
    DTAPIMETHOD(void) SetDailyCheckTime(int time_int) = 0;
    DTAPIMETHOD(void) SetQueryTimeoutMs(int ms) = 0;

    DTAPIMETHOD(const char *) Start() = 0;
    DTAPIMETHOD(void) Stop() = 0;
};
typedef RefCountedPtrT<AutoSubscriber> AutoSubscriberPtr;

class IOManager : public RefCounted {
public:
	PublisherPtr CreatePublisher(PublisherType type);
	PublisherPtr GetPublisherByType(PublisherType type);
	SubscriberPtr CreateSubscriber(MessageHandlerBase* handler, bool multithread_callback = false);
    DTAPIMETHOD(AutoSubscriberPtr) CreateAutoSubscriber(MessageHandlerBase* handler, bool multithread_callback = false) = 0;
	DTAPIMETHOD(bool) RegisterMessage(uint8_t sid, uint16_t sver, uint16_t mid) = 0;
	template <class T>
	bool RegisterMessage() {
		return RegisterMessage(T::ServiceID, T::ServiceVer, T::MessageID);
	}
	DTAPIMETHOD(void) AsyncResponse(Publisher* publisher, RefCounted* client, const MDLMessage*) = 0;
	DTAPIMETHOD(void) AsyncPublish(const MDLMessage*) = 0;
	DTAPIMETHOD(void) SyncPublish(const MDLMessage*) = 0;
	DTAPIMETHOD(void) EnableLog(const char* log_name = "mdl", bool console = false) = 0;
	DTAPIMETHOD(void) Shutdown() = 0;

	DTAPIMETHOD(void) SetWorkersProxy(IOManager*) = 0;
protected:
	DTAPIMETHOD(Publisher*) _CreatePublisher(PublisherType type) = 0;
	DTAPIMETHOD(Publisher*) _GetPublisherByType(PublisherType type) = 0;
	DTAPIMETHOD(Subscriber*) _CreateSubscriber(MessageHandlerBase* handler, bool multithread_callback) = 0;
};
typedef RefCountedPtrT<IOManager> IOManagerPtr;
extern "C" IOManager* DTAPIDLLCALL DllCreateIOManager(uint32_t version, int n_work_threads, int io_threads);


///////////////////////////////////////////////////////////////////////////////////////////////////////////
// client derive this class to handler individual service message
class MessageHandler : public MessageHandlerBase {
public:
	// handle network events, such as connecting, connect fail, disconnected
	virtual void OnMDLAPIMessage(const MDLMessage* msg) {
	}
	// handle service response
	virtual void OnMDLSysMessage(const MDLMessage* msg) {
	}
	// handle shanghai level1 message
	virtual void OnMDLSHL1Message(const MDLMessage* msg) {
	}
	// handle shanghai level2 message
	virtual void OnMDLSHL2Message(const MDLMessage* msg) {
	}
	// handle shenzhen level1 message
	virtual void OnMDLSZL1Message(const MDLMessage* msg) {
	}
	// handle shenzhen level2 message
	virtual void OnMDLSZL2Message(const MDLMessage* msg) {
	}
	// handle dce message
	virtual void OnMDLDCEMessage(const MDLMessage* msg) {
	}
	// handle dcel2 message
	virtual void OnMDLDCEL2Message(const MDLMessage* msg) {
	}
	// handle shfe message
	virtual void OnMDLSHFEMessage(const MDLMessage* msg) {
	}
    // handle shfe l2 message
	virtual void OnMDLSHFEL2Message(const MDLMessage* msg) {
	}
	// handle czce message
	virtual void OnMDLCZCEMessage(const MDLMessage* msg) {
	}
	// handle czce message
	virtual void OnMDLCZCEL2Message(const MDLMessage* msg) {
	}
	// handle cffex message
	virtual void OnMDLCFFEXMessage(const MDLMessage* msg) {
	}
	// handle hkex message
	virtual void OnMDLHKExMessage(const MDLMessage* msg) {
	}
		// handle gfex message
	virtual void OnMDLGFEXMessage(const MDLMessage* msg) {
	}
	// handle gfexl2 message
	virtual void OnMDLGFEXL2Message(const MDLMessage* msg) {
	}
	// handle swg message
	virtual void OnMDLSWGMessage(const MDLMessage* msg) {
	}
	virtual void OnMDLNEEQMessage(const MDLMessage* msg) {
	}
	virtual void OnMDLBARMessage(const MDLMessage* msg) {
	}
    virtual void OnMDLSHNYMessage(const MDLMessage* msg) {}
    virtual void OnMDLDigiCurMessage(const MDLMessage* msg) {}
    virtual void OnMDLCSIMessage(const MDLMessage* msg) {}
    virtual void OnMDLCNIMessage(const MDLMessage* msg) {}
    virtual void OnMDLCFFEXL2Message(const MDLMessage* msg) {}
    virtual void OnMDLSGEMessage(const MDLMessage* msg) {}


protected:
	DTAPIMETHOD(void) OnMessage(Subscriber* sender, const MDLMessage* msg) {
		switch (msg->GetHead()->ServiceID) {
		case MDLSID_MDL_API: OnMDLAPIMessage(msg); break;
		case MDLSID_MDL_SYS: OnMDLSysMessage(msg); break;
		case MDLSID_MDL_SHL1: OnMDLSHL1Message(msg); break;
		case MDLSID_MDL_SHL2: OnMDLSHL2Message(msg); break;
		case MDLSID_MDL_SZL1: OnMDLSZL1Message(msg); break;
		case MDLSID_MDL_SZL2: OnMDLSZL2Message(msg); break;
		case MDLSID_MDL_SHFE: OnMDLSHFEMessage(msg); break;
        case MDLSID_MDL_SHFEL2: OnMDLSHFEL2Message(msg); break;
		case MDLSID_MDL_CZCE: OnMDLCZCEMessage(msg); break;
		case MDLSID_MDL_CZCEL2: OnMDLCZCEL2Message(msg); break;
		case MDLSID_MDL_CFFEX: OnMDLCFFEXMessage(msg); break;
		case MDLSID_MDL_DCE: OnMDLDCEMessage(msg); break;
		case MDLSID_MDL_DCEL2: OnMDLDCEL2Message(msg); break;
		case MDLSID_MDL_HKEX: OnMDLHKExMessage(msg); break;
		case MDLSID_MDL_SWG: OnMDLSWGMessage(msg); break;
		case MDLSID_MDL_BAR: OnMDLBARMessage(msg); break;
		case MDLSID_MDL_NEEQ: OnMDLNEEQMessage(msg); break;
        case MDLSID_MDL_SHNY: OnMDLSHNYMessage(msg); break;
        case MDLSID_MDL_DIGICUR: OnMDLDigiCurMessage(msg); break;
        case MDLSID_MDL_CSI: OnMDLCSIMessage(msg); break;
        case MDLSID_MDL_CNI: OnMDLCNIMessage(msg); break;
        case MDLSID_MDL_CFFEXL2: OnMDLCFFEXL2Message(msg); break;
        case MDLSID_MDL_GFEX: OnMDLGFEXMessage(msg); break;
        case MDLSID_MDL_GFEXL2: OnMDLGFEXL2Message(msg); break;
        case MDLSID_MDL_SGE: OnMDLSGEMessage(msg); break;
		default:
			break;
		}
	}
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////

inline RefCountedPtrT<MDLMessage> MDLMessage::Copy() const {
	return PtrFromReturn(_Copy());
}
inline SubscriberPtr IOManager::CreateSubscriber(MessageHandlerBase* handler, bool multithread_callback) {
	return PtrFromReturn(_CreateSubscriber(handler, multithread_callback));
}
inline PublisherPtr IOManager::GetPublisherByType(PublisherType type) {
	return PtrFromReturn(_GetPublisherByType(type));
}
inline PublisherPtr IOManager::CreatePublisher(PublisherType type) {
	return PtrFromReturn(_CreatePublisher(type));
}
inline uint32_t MDLMessage::GetBodySize() const {
	return GetHead()->MessageSize - GetHead()->HeadSize;
}
inline IOManagerPtr CreateIOManager(int work_threads, int io_threads = 1) {
	return PtrFromReturn(DllCreateIOManager(MDL_VERSION, work_threads, io_threads));
}

} // namespace mdl
} // namespace datayes
