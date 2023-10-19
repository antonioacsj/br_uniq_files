package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type TFile struct {
	path string
	info os.FileInfo
	hash string
}

type TGrupoFile struct {
	tamanho  int64
	hash     string
	arquivos []TFile
}
type TchanStruc struct {
	idx_grupo   int32
	idx_arquivo int32
	path        string
}

var Todos []TGrupoFile
var MesmoTamanho []TGrupoFile
var Duplicados []TGrupoFile
var GrupoFilesUnicosFinal []TFile
var done = make(chan bool)
var canal = make(chan TchanStruc)

func Work(idx_consumidor int, wg *sync.WaitGroup, produtor <-chan TchanStruc) {
	/*
		This func receives files from the channel, to do the hash calcs
		Must be several workers executing this
	*/
	defer wg.Done()
	for fileAProcessar := range produtor {
		MesmoTamanho[fileAProcessar.idx_grupo].arquivos[fileAProcessar.idx_arquivo].hash = hashFile5Pulos(fileAProcessar.path)
		fmt.Printf(".")
	}
}

func Produce(wg *sync.WaitGroup) <-chan TchanStruc {
	/*
		This func send files to a channel, to a consumer do hash calcs
	*/
	chanel := make(chan TchanStruc)
	go func() {
		for i := 0; i < len(MesmoTamanho); i++ {
			listaArquivos := MesmoTamanho[i].arquivos
			for j := 0; j < len(listaArquivos); j++ {
				chanel <- TchanStruc{int32(i), int32(j), listaArquivos[j].path}
			}
		}

		wg.Done()
		close(chanel)
	}()
	return chanel
}

func main() {

	if len(os.Args) < 2 {
		fmt.Print("Path not given")
		return
	}

	dirAntigo := strings.TrimPrefix(strings.TrimSuffix(os.Args[1], "\\"), ".\\")
	info, err := os.Stat(dirAntigo)
	if err != nil {
		fmt.Printf("Path:'%s' don´t exists. It must be a directory.", dirAntigo)
		return
	}
	if !info.IsDir() {
		fmt.Printf("Path:'%s' exists, but is not a directory.", dirAntigo)
		return
	}
	dirNovo := strings.TrimSuffix(dirAntigo, "\\") + "_uniques"

	info, err = os.Stat(dirNovo)
	if err == nil {
		fmt.Printf("Path:'%s' already exists. Rename it and try again.", dirNovo)
		return
	}
	fmt.Printf("\nOriginal Path:'%s'", dirAntigo)
	fmt.Printf("\nUnique files will be moved to Path:'%s'", dirNovo)

	nConsumers := runtime.NumCPU()
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Printf("\n\n**Loading files... (step One):")
	carregaArquivos()
	fmt.Printf("\n\n**(step One) finished!")

	fmt.Printf("\n\n**Selecting uniques by size (step Two):")
	selecionaTamanhosUnicos()
	fmt.Printf("\n\n**(step Two) finished!")

	fmt.Printf("\n\n**Heavy work (step Three):")
	produtor_wg := &sync.WaitGroup{}
	consumidor_wg := &sync.WaitGroup{}

	produtor_wg.Add(1)
	produtor := Produce(produtor_wg)

	for i := 0; i < nConsumers; i++ {
		consumidor_wg.Add(1)
		go Work(i, consumidor_wg, produtor)

	}
	produtor_wg.Wait()
	consumidor_wg.Wait()
	fmt.Printf("\n\n**(step Three) finished!")

	var arquivo_unico bool
	fmt.Printf("\n\n**Selecting uniques by hash (step Four):")
	for i := 0; i < len(MesmoTamanho); i++ {
		if len(MesmoTamanho[i].arquivos) > 1 {

			for j := 0; j < len(MesmoTamanho[i].arquivos); j++ {
				fmt.Printf(".")
				arquivo_unico = true
				for k := j + 1; k < len(MesmoTamanho[i].arquivos); k++ {
					if MesmoTamanho[i].arquivos[j].hash == MesmoTamanho[i].arquivos[k].hash {
						arquivo_unico = false
						break
					}
				}
				if arquivo_unico {
					GrupoFilesUnicosFinal = append(GrupoFilesUnicosFinal, MesmoTamanho[i].arquivos[j])
				}
			}
		}
	}
	fmt.Printf("\n\n**(step Four) finished!")
	fmt.Printf("\n\n Total of Uniques: %d", len(GrupoFilesUnicosFinal))

	fmt.Printf("\n\n**Moving Uniques (step Five):\n")
	processaResultado(dirAntigo, dirNovo)
	fmt.Printf("\n\n**(step Five) finished!")

}

func processaResultado(dirAntigo, dirNovo string) {

	for i := 0; i < len(GrupoFilesUnicosFinal); i++ {
		f_antigo := GrupoFilesUnicosFinal[i].path
		f_novo := strings.Replace(f_antigo, dirAntigo, dirNovo, -1)
		fmt.Printf("\n\n(Old): %s\n(New): %s", f_antigo, f_novo)
		err := ensureDir(filepath.Dir(f_novo))
		if err == nil {
			e := os.Rename(f_antigo, f_novo)
			if e != nil {
				log.Fatal(e)
			}
		} else {
			log.Fatal(err)
		}

	}
}

func carregaArquivos() {
	/* Gera MesmoTamanho, contendo grupos de arquivos do mesmo tamanho */

	n_files := 0
	var tfile TFile
	filepath.Walk(os.Args[1],
		func(path string, info os.FileInfo, err error) error {
			if info.Size() > 0 && !info.IsDir() {
				n_files++
				fmt.Printf("\n%s", path)
				tfile.info = info
				tfile.path = path
				AddTArquivoTGrupo(tfile)
			}
			return nil
		})
	fmt.Printf("\nLoaded %d files in %d group of same size.", n_files, len(Todos))

}

func selecionaTamanhosUnicos() {

	for i := 0; i < len(Todos); i++ {
		fmt.Printf(".")
		if len(Todos[i].arquivos) > 1 {
			MesmoTamanho = append(MesmoTamanho, Todos[i])
		} else {
			GrupoFilesUnicosFinal = append(GrupoFilesUnicosFinal, Todos[i].arquivos[0])
		}
	}

}

func AddTArquivoTGrupo(tfile TFile) {

	var achou bool
	achou = false

	for i := 0; i < len(Todos); i++ {
		for j := 0; j < len(Todos[i].arquivos); j++ {
			if Todos[i].arquivos[j].info.Size() == tfile.info.Size() {
				Todos[i].arquivos = append(Todos[i].arquivos, tfile)
				return
			}
		}

	}

	if !achou {
		var grupoFile TGrupoFile
		grupoFile.tamanho = tfile.info.Size()
		grupoFile.arquivos = append(grupoFile.arquivos, tfile)
		Todos = append(Todos, grupoFile)
	}

}

func hashFile5Pulos(arquivo string) string {

	f, err := os.Open(arquivo)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	blockSize := 1024 * 32 //32 Kb
	if fi.Size() <= int64(blockSize*5) {
		return hashFileCompleto(arquivo)
	}

	tamanho_pulo := fi.Size() / int64(5)

	bloco := make([]byte, blockSize)

	h := sha256.New()
	for j := int64(0); j < 5; j++ {
		//fmt.Println("PEdaco", j)
		n, err := f.Read(bloco)
		if n > 0 && (err == nil || err == io.EOF) {
			h.Write(bloco)
		} else {
			log.Fatal(err)
		}
		f.Seek(tamanho_pulo*j, 0)
	}
	/*
		if _, err := io.Copy(h, f); err != nil {
			log.Fatal(err)
		}*/

	return fmt.Sprintf("%x", h.Sum(nil))
}

func hashFileCompleto(arquivo string) string {
	f, err := os.Open(arquivo)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func ensureDir(dirName string) error {
	err := os.MkdirAll(dirName, os.ModeDir)
	if err == nil {
		return nil
	}
	if os.IsExist(err) {
		// check that the existing path is a directory
		info, err := os.Stat(dirName)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("path exists but is not a directory")
		}
		return nil
	}
	return err
}
