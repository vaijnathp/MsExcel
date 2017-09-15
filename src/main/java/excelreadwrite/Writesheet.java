package excelreadwrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Writesheet {
    public static void main(String[] args) throws Exception {
        XSSFRow row;

        Path pt = new Path(args[0]);
        FileSystem fs = FileSystem.get(new Configuration());
        XSSFWorkbook workbook = new XSSFWorkbook(fs.open(pt));
        XSSFSheet sheet = workbook.createSheet(args[1]);
        XSSFSheet spreadsheet = workbook.getSheetAt(0);
        Iterator<Row> rowIterator = spreadsheet.iterator();
        while (rowIterator.hasNext()) {
            row = (XSSFRow) rowIterator.next();
            Iterator<Cell> cellIterator = row.cellIterator();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                switch (cell.getCellTypeEnum()) {
                    case NUMERIC:
                        System.out.print(
                                cell.getNumericCellValue() + " \t\t ");
                        break;
                    case STRING:
                        System.out.print(
                                cell.getStringCellValue() + " \t\t ");
                        break;
                }
            }
            System.out.println();
        }


        //This data needs to be written (Object[])
        Map<String, Object[]> empinfo = new TreeMap<String, Object[]>();
        empinfo.put("1", new Object[]{"EMP ID", "EMP NAME", "DESIGNATION"});
        empinfo.put("2", new Object[]{"tp01", "Gopal", "Technical Manager"});
        empinfo.put("3", new Object[]{"tp02", "Manisha", "Proof Reader"});
        empinfo.put("4", new Object[]{"tp03", "Masthan", "Technical Writer"});
        empinfo.put("5", new Object[]{"tp04", "Satish", "Technical Writer"});
        empinfo.put("6", new Object[]{"tp05", "Krishna", ""});
        //Iterate over data and write to sheet
        Set<String> keyid = empinfo.keySet();
        int rowid = 3;
        XSSFCellStyle cellStyle = workbook.createCellStyle();
        XSSFFont cellFont = workbook.createFont();
        for (String key : keyid) {
            row = sheet.createRow(rowid++);
            Object[] objectArr = empinfo.get(key);
            int cellid = 3;
            for (Object obj : objectArr) {
                Cell cell = row.createCell(cellid++);
                cellStyle.setFont(getCellFont(cellFont, args[2]));
                cell.setCellStyle(getCellStyle(cellStyle, args[3]));
                cell.setCellValue((String) obj);
            }
        }
        //Write the workbook in file system
        OutputStream out = fs.create(new Path("Writesheet.xlsx"));
        workbook.write(out);
        out.close();
        fs.close();
        System.out.println(
                "Writesheet.xlsx written successfully");
    }

    private static Font getCellFont(XSSFFont cellFont, String arg) {
        System.out.println(arg);
        String[] sarr = arg.split("|");
        for (String s : sarr) {
            if (s.equals("BOLD=TRUE")) {
                cellFont.setBold(true);

            } else if (s.equals("ITALIC=TRUE")) {
                cellFont.setItalic(true);
            }
            cellFont.setFontHeightInPoints((short) 20);
        }
        return cellFont;
    }

    private static CellStyle getCellStyle(XSSFCellStyle cellStyle, String arg) {
        System.out.println(arg);

        String[] sarr = arg.split("|");
        for (String s : sarr) {
            if (s.equals("ALIGNMENT=CENTER")) {
                cellStyle.setAlignment(HorizontalAlignment.CENTER);
            } else if (s.equals("BORDER=ALL")) {
                cellStyle.setBorderTop(BorderStyle.THIN);
                cellStyle.setBorderBottom(BorderStyle.THIN);
                cellStyle.setBorderLeft(BorderStyle.THIN);
                cellStyle.setBorderRight(BorderStyle.THIN);
            } else if (s.equals("BACKGROUND=ROYAL_BLUE")) {
                cellStyle.setFillBackgroundColor(IndexedColors.ROYAL_BLUE.getIndex());
            } else if (s.equals("Foreground=RED")) {
                cellStyle.setFillBackgroundColor(IndexedColors.RED.getIndex());
            }
        }
        return cellStyle;
    }

}
