package main

import (
	"encoding/xml"
	"io"
	"io/ioutil"
)

type CompleteMultipartUpload struct {
	XMLName     xml.Name `xml:"CompleteMultipartUpload"`
	PartNumbers []int    `xml:"Part>PartNumber"`
}

func parseCompleteMultipartUpload(r io.Reader) (*CompleteMultipartUpload, error) {
	out, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	//fmt.Println(string(out))
	var cmu CompleteMultipartUpload
	if err := xml.Unmarshal(out, &cmu); err != nil {
		return nil, err
	}
	return &cmu, nil
}
