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

type Delete struct {
	XMLName xml.Name `xml:"Delete"`
	Keys    []string `xml:"Object>Key"`
}

func parseXML(r io.Reader, dat interface{}) error {
	out, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	if err := xml.Unmarshal(out, &dat); err != nil {
		return err
	}
	return nil
}

func parseCompleteMultipartUpload(r io.Reader) (*CompleteMultipartUpload, error) {
	var cmu CompleteMultipartUpload
	err := parseXML(r, &cmu)
	if err != nil {
		return nil, err
	}
	return &cmu, nil
}

func parseDelete(r io.Reader) (*Delete, error) {
	var del Delete
	err := parseXML(r, &del)
	if err != nil {
		return nil, err
	}
	return &del, nil
}
