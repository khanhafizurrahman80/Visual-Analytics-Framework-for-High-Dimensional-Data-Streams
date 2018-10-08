package com.spark_spring_kafka_viz.file_service_implementation;

import com.spark_spring_kafka_viz.POJO.FileDescription;
import com.spark_spring_kafka_viz.POJO.ResponseMetaData;
import com.spark_spring_kafka_viz.file_service_interface.FileServiceInterface;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Created by khanhafizurrahman on 10/6/18.
 */
@Service
public class FileServiceImplementation implements FileServiceInterface {

    final static String UPLOADED_FOLDER = "/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/UploadFiles/";

    @Override
    public ArrayList<FileDescription> findAll() {
        File folder = new File (UPLOADED_FOLDER);
        File[] listOfFiles = folder.listFiles();
        ArrayList<FileDescription>  ListOfFilesInDirectory = new ArrayList <FileDescription> ();
        for (int i= 0; i < listOfFiles.length; i++) {
            System.out.println(listOfFiles[i].getName());
            FileDescription detailsOfFile = new FileDescription((long) i,listOfFiles[i].getName());
            ListOfFilesInDirectory.add(detailsOfFile);
        }
        return ListOfFilesInDirectory;
    }

    @Override
    public ResponseMetaData save(MultipartFile file){
        Path rootLocation = Paths.get(UPLOADED_FOLDER);
        String fileName = StringUtils.cleanPath(file.getOriginalFilename());
        try (InputStream inputStream = file.getInputStream()){
            Files.copy(inputStream, rootLocation.resolve(fileName),
                    StandardCopyOption.REPLACE_EXISTING);
        }catch (IOException ie){
            ie.printStackTrace();
        }

        ResponseMetaData metaData = new ResponseMetaData();
        metaData.setMessage("success");
        metaData.setStatus(200);
        return metaData;
    }

    private void saveUploadedFiles(MultipartFile multipartFile) throws IOException{
        byte[] bytes = multipartFile.getBytes();
        Path path = Paths.get(UPLOADED_FOLDER + multipartFile.getOriginalFilename());
        Files.write(path, bytes);
    }

    @Override
    public List<List<String>> getHeadersName(String inputFilePath) {
        String absolutePath = UPLOADED_FOLDER + inputFilePath;
        List<String> headerNames = new ArrayList<String>();
        List<List<String>> schemasOfFile = new ArrayList<>();
        String headerNamesString = "";
        try {
            File inputF = new File(absolutePath);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));

            headerNames = Stream.of(br.readLine()).map(line -> line.split(","))
                    .flatMap(Arrays:: stream).collect(Collectors.toList());

        } catch (Exception e) {
            // TODO: handle exception
        }
        System.out.println(headerNames.size());
        int i =1;
        for (String s : headerNames){
            System.out.println(i);
            i++;
            System.out.println(s);
        }
        List<String> fileTypes = contentsOfFirstLine(inputFilePath);
        System.out.println(fileTypes.size());
        schemasOfFile.add(headerNames);
        schemasOfFile.add(fileTypes);
        return schemasOfFile;
    }

    private List<String> contentsOfFirstLine(String inputFilePath){
        String absolutePath = UPLOADED_FOLDER + inputFilePath;
        List<String> firstLineList = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();
        try {
            File inputF = new File(absolutePath);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            br.readLine();
            firstLineList = Stream.of(br.readLine()).map(line -> line.split(","))
                    .flatMap(Arrays:: stream).collect(Collectors.toList());
            //firstLineList.forEach(line -> System.out.println(line.getClass().getName()));
            for (String s: firstLineList){
                fieldTypes.add(checkwhetherStringConvertableOrnot(s));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fieldTypes;
    }

    private String checkwhetherStringConvertableOrnot(String s) {
        final String Digits     = "(\\p{Digit}+)";
        final String HexDigits  = "(\\p{XDigit}+)";
        // an exponent is 'e' or 'E' followed by an optionally
        // signed decimal integer.
        final String Exp        = "[eE][+-]?"+Digits;
        final String fpRegex    =
                ("[\\x00-\\x20]*"+  // Optional leading "whitespace"
                        "[+-]?(" + // Optional sign character
                        "NaN|" +           // "NaN" string
                        "Infinity|" +      // "Infinity" string

                        // A decimal floating-point string representing a finite positive
                        // number without a leading sign has at most five basic pieces:
                        // Digits . Digits ExponentPart FloatTypeSuffix
                        //
                        // Since this method allows integer-only strings as input
                        // in addition to strings of floating-point literals, the
                        // two sub-patterns below are simplifications of the grammar
                        // productions from section 3.10.2 of
                        // The Java Language Specification.

                        // Digits ._opt Digits_opt ExponentPart_opt FloatTypeSuffix_opt
                        "((("+Digits+"(\\.)?("+Digits+"?)("+Exp+")?)|"+

                        // . Digits ExponentPart_opt FloatTypeSuffix_opt
                        "(\\.("+Digits+")("+Exp+")?)|"+

                        // Hexadecimal strings
                        "((" +
                        // 0[xX] HexDigits ._opt BinaryExponent FloatTypeSuffix_opt
                        "(0[xX]" + HexDigits + "(\\.)?)|" +

                        // 0[xX] HexDigits_opt . HexDigits BinaryExponent FloatTypeSuffix_opt
                        "(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")" +

                        ")[pP][+-]?" + Digits + "))" +
                        "[fFdD]?))" +
                        "[\\x00-\\x20]*"
                );// Optional trailing "whitespace"
        if (Pattern.matches(fpRegex, s))
            return "double"; // Will not throw NumberFormatException
        else {
            // Perform suitable alternative action
            return "string";
        }
    }

    @Override
    public List<String> contentsInJson(String inputFilePath) {
        String absolutePath = UPLOADED_FOLDER + inputFilePath;
        List<String> fileContents = new ArrayList<String>();
        try {
            Stream<String> csv_data_File_Stream = Files.lines(Paths.get(absolutePath)).skip(1);
            fileContents = csv_data_File_Stream.collect(Collectors.toList());
            fileContents.forEach(System.out::println);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return fileContents;
    }


    @Override
    public void preprocessOriginalFile(String inputFilePath) {
        System.out.println("preprocessing file:: " + inputFilePath);
        String absolutePath = UPLOADED_FOLDER + inputFilePath;
        if (checkAlreadyModifiedOrnot(absolutePath)){
            System.out.println("already exists!!!");
            return;
        }
        String outputFilePath = inputFilePath.replace(".csv","") + "_output.csv";
        String absoluteOutputFilePath = UPLOADED_FOLDER + outputFilePath;
        checkFileExistOrNot(absoluteOutputFilePath);
        ICsvMapReader mapReader = null;
        ICsvMapWriter mapWriter = null;
        CsvPreference prefs = CsvPreference.STANDARD_PREFERENCE;
        try {
            mapReader = new CsvMapReader(new FileReader(absolutePath),prefs);
            mapWriter = new CsvMapWriter(new FileWriter(absoluteOutputFilePath),prefs);
            final String[] readHeader = mapReader.getHeader(true);
            final String[] writeHeader = new String[readHeader.length + 1];
            System.arraycopy(readHeader, 0, writeHeader, 0, readHeader.length);
            final String defaultHeader = "defaultHeader";
            writeHeader[writeHeader.length -1] = defaultHeader;
            mapWriter.writeHeader(writeHeader);
            Map<String, String> row;
            while( (row = mapReader.read(readHeader)) !=null) {
                row.put(defaultHeader,"defaultValue");
                mapWriter.write(row, writeHeader);
            }
            renameFileandDeleteExistingFile(inputFilePath, outputFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();;
        } finally {
            if (mapReader != null){
                try {
                    mapReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (mapWriter != null) {
                try {
                    mapWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean checkAlreadyModifiedOrnot(String absolutePath) {
        List<String> headerNames = new ArrayList<>();
        try {
            File inputF = new File(absolutePath);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));

            headerNames = Stream.of(br.readLine()).map(line -> line.split(","))
                    .flatMap(Arrays:: stream).collect(Collectors.toList());

        } catch (Exception e) {
            // TODO: handle exception
        }

        if (headerNames.contains(("defaultHeader"))) {
            return true;
        } else {
            return false;
        }
    }

    private void renameFileandDeleteExistingFile(String inputFilePath, String outputFilePath) {
        Path source = Paths.get(UPLOADED_FOLDER + outputFilePath);
        try {
            Files.move(source, source.resolveSibling(inputFilePath), REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkFileExistOrNot(String absoluteOutputFilePath) {
        File outputFile = new File(absoluteOutputFilePath);
        if (outputFile.exists() && !outputFile.isDirectory()){
            outputFile.delete();
        }
    }


}
