package aea_api

import (
  "fmt"
  "os"
  "encoding/binary"
  proto "github.com/golang/protobuf/proto"
  "math"
  "math/rand"
  "time"
  "errors"
  "syscall"
  "strconv"
)

/*
  
  AeaApi type

*/

type AeaApi struct {
  msgin_path  string
  msgout_path string
  msgin       *os.File
  msgout      *os.File
  out_queue   chan *Envelope
  closing     bool
}

func (aea AeaApi) Put(envelope *Envelope) error {
  return write_envelope(aea.msgout, envelope)
}

func (aea *AeaApi) Get() *Envelope {
  return <- aea.out_queue
}

func (aea *AeaApi) Queue() <-chan *Envelope {
  return aea.out_queue
}

func (aea *AeaApi) Stop() {
  //TOFIX(LR) stop the listen_for_envelopes goroutine
  aea.closing = true
  aea.stop()
  // Don't close the queue
}
func (aea *AeaApi) Init(sandbox bool) error {
  //TOFIX(LR) Go doesn't support neither default params nor constructors
  if sandbox {
    var err error
    fmt.Println("[go] warning running in sandbox mode")
    aea.msgin_path, aea.msgout_path, err = setup_aea_sandbox()
    if err != nil {
      return err 
    }   
  } else {
    aea.msgin_path  = os.Getenv("AEA_TO_NOISE")
    aea.msgout_path = os.Getenv("NOISE_TO_AEA")
  }

  if aea.msgin_path == "" || aea.msgout_path == "" {
    return errors.New("Couldn't get AEA pipes.")
  }

  fmt.Println("[go] opening pipes", aea.msgout_path, ",", aea.msgin_path,"...")
  //TOFIX(LR) needed, short declaration produces compilation error 'non-name on left side of :='
  var erro, erri error
  aea.msgout, erro = os.OpenFile(aea.msgout_path, os.O_WRONLY, os.ModeNamedPipe)
  aea.msgin,  erri = os.OpenFile(aea.msgin_path, os.O_RDONLY, os.ModeNamedPipe)
  fmt.Println("[go] intialized", aea.msgin, " and", aea.msgout)

  if erri != nil || erro != nil {
    fmt.Println("[go] error when opening pipes", erri, erro)
    if erri != nil {
      return erri
    }
    return erro
  }

  aea.closing = false
  //TOFIX(LR) trade-offs between bufferd vs unbuffered channel
  aea.out_queue = make(chan *Envelope, 10) 
  go aea.listen_for_envelopes()
  fmt.Println("[go] info aea started receiving messages")

  return nil
}

func (aea *AeaApi) listen_for_envelopes() {
  //TOFIX(LR) add an exit strategy
  for {
    envel, err := read_envelope(aea.msgin)
    if err != nil {
      fmt.Println("[go] error while receiving envelope", err)
      fmt.Println("[go] disconnecting")
      // TOFIX(LR) see above
      if !aea.closing {
        aea.stop()
      }
      return
    }
    aea.out_queue <- envel
    if aea.closing {
      return
    }
  }
}

func (aea *AeaApi) stop() {
  aea.msgin.Close()
  aea.msgout.Close()
}

/* 
  
  Pipes helpers

 */

func write(pipe *os.File, data []byte) error {
  size := uint32(len(data))
  buf := make([]byte, 4)
  binary.BigEndian.PutUint32(buf, size)
  _, err := pipe.Write(buf)
  if err != nil {
    return err
  }
  _, err = pipe.Write(data)
  return err
}

func read(pipe *os.File) ([]byte, error) {
  fmt.Println("[go] reading from ", pipe)
  buf := make([]byte, 4)
  _, err := pipe.Read(buf)
  if err != nil {
    fmt.Println("[go] erro while receiving size", err)
    return buf, err
  }
  size := binary.BigEndian.Uint32(buf)
  fmt.Println("[go] received size", size)

  buf = make([]byte, size)
  _, err = pipe.Read(buf)
  return buf, err
}

func write_envelope(pipe *os.File, envelope *Envelope) error {
  data, err := proto.Marshal(envelope)
  if err != nil {
    fmt.Println("[go] Error when serializing envelope", envelope, ":", err)
    return err
  }
  return write(pipe, data)
}

func read_envelope(pipe *os.File) (*Envelope, error) {
  envelope := &Envelope{}
  data, err := read(pipe)
  if err != nil {
    fmt.Println("[go] Error while receiving data :", err)
    return envelope, err
  }
  err = proto.Unmarshal(data, envelope)
  return envelope, err
}

/*
  
  Sandbox

*/

func setup_aea_sandbox() (string, string, error) {
  ROOT_PATH := "/tmp/aea_sandbox"+ string(time.Now().Unix())
  msgin_path  := ROOT_PATH + ".in"
  msgout_path := ROOT_PATH + ".out"
  // create pipes
  if _, err := os.Stat(msgin_path); ! os.IsNotExist(err) {
    os.Remove(msgin_path)
  }
  if _, err := os.Stat(msgout_path); ! os.IsNotExist(err) {
    os.Remove(msgout_path)
  }
  erri := syscall.Mkfifo(msgin_path, 0666)
  erro := syscall.Mkfifo(msgout_path, 0666)
  if erri != nil || erro != nil {
    fmt.Println("[go] sandbox error setting up pipes:", erri, erro)
    if erri != nil {
      return  "", "", erri
    }
    return "", "", erro 
  }
  go run_aea_sandbox(msgin_path, msgout_path)
  return msgin_path, msgout_path, nil
}


func run_aea_sandbox(msgin_path string, msgout_path string) error {
  // open pipe
  msgout, erro := os.OpenFile(msgout_path, os.O_RDONLY, os.ModeNamedPipe)
  msgin,  erri := os.OpenFile(msgin_path, os.O_WRONLY, os.ModeNamedPipe)
  if erri != nil || erro != nil {
    fmt.Println("[go] sandbox error when opening pipes", erri, erro)
    if erri != nil {
      return erri
    } else {
      return erro
    }
  }
  // consume envelopes
  go func() {
    for {
      envel, err := read_envelope(msgout)
      if err != nil {
        fmt.Println("[go] sandbox stopped receiving envelopes")
        return
      }
      fmt.Println("[go] sandbox received:", envel)
    }
  }()
  // produce envelopes
  go func() {
    i := 1
    for {
      time.Sleep(time.Duration((rand.Intn(3000)+1500))*time.Millisecond)
      envel := &Envelope{"none", "golang", "fetchai/default:0.1.0", []byte("\x08\x01*\x07\n\x05Message from sandbox "+strconv.Itoa(i)), ""}
      err := write_envelope(msgin, envel)
      if err != nil {
        fmt.Println("[go] sandbox stopped producing envelopes")
        return
      }
      i += 1
    }
  }()

  return nil
}


/*
  
  Protobuf generated Envelope

*/

// Code generated by protoc-gen-go. DO NOT EDIT.
// source: pocs/p2p_noise_pipe/envelope.proto

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Envelope struct {
  To         string `protobuf:"bytes,1,opt,name=to" json:"to,omitempty"`
  Sender     string `protobuf:"bytes,2,opt,name=sender" json:"sender,omitempty"`
  ProtocolId string `protobuf:"bytes,3,opt,name=protocol_id,json=protocolId" json:"protocol_id,omitempty"`
  Message    []byte `protobuf:"bytes,4,opt,name=message,proto3" json:"message,omitempty"`
  Uri        string `protobuf:"bytes,5,opt,name=uri" json:"uri,omitempty"`
}

func (m *Envelope) Reset()                    { *m = Envelope{} }
func (m *Envelope) String() string            { return proto.CompactTextString(m) }
func (*Envelope) ProtoMessage()               {}
func (*Envelope) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Envelope) GetTo() string {
  if m != nil {
    return m.To
  }
  return ""
}


func (m *Envelope) GetSender() string {
  if m != nil {
    return m.Sender
  }
  return ""
}

func (m *Envelope) GetProtocolId() string {
  if m != nil {
    return m.ProtocolId
  }
  return ""
}

func (m *Envelope) GetMessage() []byte {
  if m != nil {
    return m.Message
  }
  return nil
}

func (m *Envelope) GetUri() string {
  if m != nil {
    return m.Uri
  }
  return ""
}

func init() {
  proto.RegisterType((*Envelope)(nil), "Envelope")
}

func init() { proto.RegisterFile("envelope.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
  // 157 bytes of a gzipped FileDescriptorProto
  0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x52, 0x2a, 0xc8, 0x4f, 0x2e,
  0xd6, 0x2f, 0x30, 0x2a, 0x88, 0xcf, 0xcb, 0xcf, 0x2c, 0x4e, 0x8d, 0x2f, 0xc8, 0x2c, 0x48, 0xd5,
  0x4f, 0xcd, 0x2b, 0x4b, 0xcd, 0xc9, 0x2f, 0x48, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x57, 0xaa,
  0xe7, 0xe2, 0x70, 0x85, 0x8a, 0x08, 0xf1, 0x71, 0x31, 0x95, 0xe4, 0x4b, 0x30, 0x2a, 0x30, 0x6a,
  0x70, 0x06, 0x31, 0x95, 0xe4, 0x0b, 0x89, 0x71, 0xb1, 0x15, 0xa7, 0xe6, 0xa5, 0xa4, 0x16, 0x49,
  0x30, 0x81, 0xc5, 0xa0, 0x3c, 0x21, 0x79, 0x2e, 0x6e, 0xb0, 0xe6, 0xe4, 0xfc, 0x9c, 0xf8, 0xcc,
  0x14, 0x09, 0x66, 0xb0, 0x24, 0x17, 0x4c, 0xc8, 0x33, 0x45, 0x48, 0x82, 0x8b, 0x3d, 0x37, 0xb5,
  0xb8, 0x38, 0x31, 0x3d, 0x55, 0x82, 0x45, 0x81, 0x51, 0x83, 0x27, 0x08, 0xc6, 0x15, 0x12, 0xe0,
  0x62, 0x2e, 0x2d, 0xca, 0x94, 0x60, 0x05, 0x6b, 0x01, 0x31, 0x93, 0xd8, 0xc0, 0xfa, 0x8c, 0x01,
  0x01, 0x00, 0x00, 0xff, 0xff, 0xaf, 0x62, 0x87, 0x61, 0xad, 0x00, 0x00, 0x00,
}

