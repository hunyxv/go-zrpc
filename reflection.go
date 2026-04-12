package zrpc

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
)

var (
	errType         = reflect.TypeOf((*error)(nil)).Elem()
	ctxType         = reflect.TypeOf((*context.Context)(nil)).Elem()
	readerType      = reflect.TypeOf((*io.Reader)(nil)).Elem()
	writeCloserType = reflect.TypeOf((*io.WriteCloser)(nil)).Elem()
	streamType      = reflect.TypeOf((*Stream)(nil)).Elem()
	bidiStreamType  = reflect.TypeOf((*BidiStream)(nil)).Elem()
)

// Method 方法描述
type Method struct {
	Name        string
	Mode        FuncMode
	ServiceName string
	MethodName  string
	Receiver    reflect.Value
	Func        reflect.Value
	ParamTypes  []reflect.Type
	ReturnTypes []reflect.Type
	HasStream   bool
}

// Service 服务描述
type Service struct {
	Name     string
	Instance reflect.Value
	Methods  map[string]*Method
}

// Registry 服务注册表
type Registry struct {
	services map[string]*Service
	rwmu     sync.RWMutex
}

// NewRegistry 创建注册表
func NewRegistry() *Registry {
	return &Registry{
		services: make(map[string]*Service),
	}
}

// Register 注册服务
func (r *Registry) Register(name string, instance interface{}, convention interface{}) error {
	if name == "" {
		return fmt.Errorf("service name cannot be empty")
	}

	instValue := reflect.ValueOf(instance)
	if instValue.IsNil() {
		return ErrInvalidServer
	}

	instType := instValue.Type()
	convType := reflect.TypeOf(convention)

	// 检查是否实现了约定接口
	if !instType.Implements(convType.Elem()) {
		return ErrNotImplements
	}

	svc := &Service{
		Name:     name,
		Instance: instValue,
		Methods:  make(map[string]*Method),
	}

	// 遍历所有方法
	for i := 0; i < instValue.NumMethod(); i++ {
		methodType := instType.Method(i)
		methodValue := instValue.Method(i)

		m, err := r.parseMethod(name, methodType, methodValue)
		if err != nil {
			return fmt.Errorf("parse method %s failed: %w", methodType.Name, err)
		}

		if m != nil {
			svc.Methods[m.MethodName] = m
		}
	}

	r.rwmu.Lock()
	r.services[name] = svc
	r.rwmu.Unlock()

	return nil
}

// parseMethod 解析方法
func (r *Registry) parseMethod(svcName string, methodType reflect.Method, methodValue reflect.Value) (*Method, error) {
	m := &Method{
		Name:        methodType.Name,
		ServiceName: svcName,
		MethodName:  svcName + "/" + methodType.Name,
		Receiver:    methodValue,
		Func:        methodValue,
	}

	mt := methodType.Type

	// 检查参数
	numIn := mt.NumIn()
	if numIn < 2 {
		return nil, ErrTooFewParam
	}

	// 收集参数类型（跳过 receiver）
	for i := 1; i < numIn; i++ {
		m.ParamTypes = append(m.ParamTypes, mt.In(i))
	}

	// 第一个参数必须是 context.Context
	if !m.ParamTypes[0].Implements(ctxType) {
		return nil, ErrInvalidParamType
	}

	// 检查返回值
	numOut := mt.NumOut()
	if numOut == 0 {
		return nil, ErrTooFewReturn
	}

	// 收集返回值类型
	for i := 0; i < numOut; i++ {
		m.ReturnTypes = append(m.ReturnTypes, mt.Out(i))
	}

	// 最后一个返回值必须是 error
	if !m.ReturnTypes[numOut-1].Implements(errType) {
		return nil, ErrInvalidResultType
	}

	// 判断调用模式
	m.Mode = r.detectMode(m)

	return m, nil
}

// detectMode 检测调用模式
func (r *Registry) detectMode(m *Method) FuncMode {
	mode := ReqRep

	// 检查参数中是否有流式类型
	for i, pt := range m.ParamTypes {
		if i == 0 {
			continue // 跳过 context
		}

		// 检查是否是 Stream 或 BidiStream
		if pt.Implements(streamType) || pt.Implements(bidiStreamType) {
			m.HasStream = true

			// 判断是哪种流式
			if pt.Implements(bidiStreamType) {
				mode = BidiStreamMode
			} else if pt.Implements(readerType) {
				// 客户端流式
				if mode == ReqStreamRep {
					mode = BidiStreamMode
				} else {
					mode = StreamReqRep
				}
			} else if pt.Implements(writeCloserType) {
				// 服务端流式
				if mode == StreamReqRep {
					mode = BidiStreamMode
				} else {
					mode = ReqStreamRep
				}
			}
		}
	}

	return mode
}

// GetService 获取服务
func (r *Registry) GetService(name string) (*Service, bool) {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()
	svc, ok := r.services[name]
	return svc, ok
}

// GetMethod 获取方法
func (r *Registry) GetMethod(fullName string) (*Method, bool) {
	parts := strings.SplitN(fullName, "/", 2)
	if len(parts) != 2 {
		return nil, false
	}

	svc, ok := r.GetService(parts[0])
	if !ok {
		return nil, false
	}

	m, ok := svc.Methods[fullName]
	return m, ok
}

// GetAllServices 获取所有服务
func (r *Registry) GetAllServices() map[string]*Service {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()

	result := make(map[string]*Service, len(r.services))
	for k, v := range r.services {
		result[k] = v
	}
	return result
}
