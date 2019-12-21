package main

import (
	// "crypto/md5"
	// "encoding/hex"
	"encoding/base64"
	"encoding/csv"
	"io"
	"log"
	"os"
  "fmt"
	"strings"
	"github.com/tg123/go-htpasswd"
)

// lookup passwords in a htpasswd file
// The entries must have been created with -s for SHA encryption

type HtpasswdFile struct {
	Users map[string]string
}

// func GetMD5Hash(text string, salt string) string {
//     hasher := md5.New()
//     hasher.Write([]byte(text))
// 		hasher.Write([]byte(salt))
//     return hex.EncodeToString(hasher.Sum(nil))
// }

func NewHtpasswdFromFile(path string) (*HtpasswdFile, error) {
	log.Printf("using htpasswd file %s", path)
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewHtpasswd(r)
}

func NewHtpasswd(file io.Reader) (*HtpasswdFile, error) {
	csv_reader := csv.NewReader(file)
	csv_reader.Comma = ':'
	csv_reader.Comment = '#'
	csv_reader.TrimLeadingSpace = true

	records, err := csv_reader.ReadAll()
	if err != nil {
		return nil, err
	}
	h := &HtpasswdFile{Users: make(map[string]string)}
	for _, record := range records {
		h.Users[record[0]] = record[1]
	}
	return h, nil
}

func (h *HtpasswdFile) Validate(authBase64 string) bool {
	decoded, _ := base64.StdEncoding.DecodeString(authBase64)
  fmt.Println("decoded string=", string(decoded))
	authString := strings.Split(string(decoded), ":")
	user := authString[0]
	password := authString[1]
	fmt.Println("username and password=", user, password)
	realPassword, exists := h.Users[user]
	if !exists {
		return false
	}
	fmt.Println("password hashed=", realPassword)
	salt := strings.Split(realPassword, "/")[1]
	salted := strings.Split(realPassword, "/")[2]
	fmt.Println("salt and salted=", salt, salted)

	passwordCheck := md5Password{
		salt: salt,
		prefix: "$apr1$",
	}

	check:= passwordCheck.MatchesPassword(hashedPass)
	fmt.Println("check=",check)
	// if realPassword[:5] == "{SHA}" {
	// 	d := sha1.New()
	// 	d.Write([]byte(password))
	// 	if realPassword[5:] == base64.StdEncoding.EncodeToString(d.Sum(nil)) {
	// 		return true
	// 	}
	// } else {
	// 	log.Printf("Invalid htpasswd entry for %s. Must be a SHA entry.", user)
	// }
	return false
}

func main() {
	fmt.Println("Starting")
  passFile, err := NewHtpasswdFromFile("/etc/nginx/.htpasswd")
  fmt.Println(passFile, err)
  validation := passFile.Validate("YWRtaW46dGhpc2lzYXRlc3Q=")
  fmt.Println("validation", validation)
  // fmt.Println("Ending")
}
