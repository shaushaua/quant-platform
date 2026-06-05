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

#ifndef DTAPIH
#define DTAPIH

namespace datayes {

#ifdef __linux__
#define DTAPICALL
#define DTAPIDLLCALL
#define DTAPIEXPORT __attribute__((visibility("default")))
#else
#define DTAPICALL	__cdecl
#define DTAPIDLLCALL __cdecl
#define DTAPIEXPORT
#endif

#define DTAPIMETHOD(type) virtual type DTAPICALL
#define DTAPIMETHODIMPL(type) type DTAPICALL

extern "C" int DTAPIDLLCALL DllInterlockedIncrement(volatile int*);
extern "C" int DTAPIDLLCALL DllInterlockedDecrement(volatile int*);

class RefCounted {
public:
	DTAPIMETHOD(void) AddRef() = 0;
	DTAPIMETHOD(int) ReleaseRef() = 0;
};

template<class Base>
class RefCountedImplT: public Base {
public:
	RefCountedImplT() :
			m_RefCount(0) {
	}

	virtual ~RefCountedImplT() {
	}

	DTAPIMETHOD(void) AddRef() {
		DllInterlockedIncrement(&m_RefCount);
	}

	DTAPIMETHOD(int) ReleaseRef() {
		int ret = DllInterlockedDecrement(&m_RefCount);
		if (ret == 0) {
			delete this;
		}
		return ret;
	}

    int GetRefCount() const { return m_RefCount; }

protected:
	volatile int m_RefCount;
};

typedef RefCountedImplT<RefCounted> RefCountedImpl;

template<class T>
class RefCountedPtrT {
public:
	typedef T ClassType;

	RefCountedPtrT() :
			m_RefObj(nullptr) {
	}

	~RefCountedPtrT() {
		if (m_RefObj != nullptr) {
			m_RefObj->ReleaseRef();
			m_RefObj = nullptr;
		}
	}

	explicit RefCountedPtrT(T* p) :
			m_RefObj(p) {
		if (m_RefObj != nullptr) {
			m_RefObj->AddRef();
		}
	}

	RefCountedPtrT(const RefCountedPtrT<T>& right) :
			m_RefObj(right.m_RefObj) {
		if (m_RefObj != nullptr) {
			m_RefObj->AddRef();
		}
	}

    RefCountedPtrT(RefCountedPtrT<T>&& right) : m_RefObj(right.m_RefObj) {
        if (right.m_RefObj != nullptr)
            right.m_RefObj = nullptr;
    }

	RefCountedPtrT<T>& operator =(const RefCountedPtrT<T>& right) {
		RefCountedPtrT<T>(right).Swap(*this);
		return *this;
	}

    RefCountedPtrT<T>& operator=(RefCountedPtrT<T>&& right) {
        return Swap(right);
    }

	bool operator ==(const RefCountedPtrT<T>& right) const {
		return m_RefObj == right.m_RefObj;
	}

	void CreateInstance() {
		T* obj = new T();
		Reset(obj);
	}

	// add ref and return ptr
	T* Duplicate() {
		if (m_RefObj != nullptr) {
			m_RefObj->AddRef();
		}

		return m_RefObj;
	}

	RefCountedPtrT<T>& Swap(RefCountedPtrT<T>& a) {
		T* tmp = a.m_RefObj;
		a.m_RefObj = m_RefObj;
		m_RefObj = tmp;
		return *this;
	}

	T& operator *() {
		return *m_RefObj;
	}

	const T& operator *() const {
		return *m_RefObj;
	}

	T* operator ->() {
		return m_RefObj;
	}

	const T* operator ->() const {
		return m_RefObj;
	}

	bool operator <(const RefCountedPtrT<T>& right) const {
		return m_RefObj < right.m_RefObj;
	}

	T* Get() {
		return (T*)m_RefObj;
	}

	const T* Get() const {
		return m_RefObj;
	}

	void Reset(T* p = nullptr) {
		RefCountedPtrT<T> a(p);
		Swap(a);
	}

	bool IsNull() const {
		return m_RefObj == nullptr;
	}

private:
	T* m_RefObj;
};

typedef RefCountedPtrT<RefCounted> RefCountedPtr;

template<class T>
inline RefCountedPtrT<T> PtrFromReturn(T* ret) {
	RefCountedPtrT<T> retPtr(ret);
	if (ret != 0) {
		ret->ReleaseRef();
	}
	return retPtr;
}

} // DTAPI

#endif
