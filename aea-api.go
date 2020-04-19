package aea-api

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



