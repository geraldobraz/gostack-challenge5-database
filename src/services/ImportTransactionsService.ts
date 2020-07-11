import { getCustomRepository, getRepository, In } from 'typeorm';
import csvParse from 'csv-parse';
import fs from 'fs';

import TransactionsRepository from '../repositories/TransactionsRepository';
import Category from '../models/Category';
import Transaction from '../models/Transaction';

interface CsvTransaction {
  title: string;
  type: 'income' | 'outcome';
  value: number;
  category: string;
}

class ImportTransactionsService {
  async execute(filePath: string): Promise<Transaction[]> {
    const transactionsRepository = getCustomRepository(TransactionsRepository);
    const categoriesRepository = getRepository(Category);

    const readStream = fs.createReadStream(filePath);
    const parser = csvParse({
      from_line: 2,
    });
    const parseCsv = readStream.pipe(parser);

    const categories: string[] = [];
    const transactions: CsvTransaction[] = [];

    parseCsv.on('data', async line => {
      const [title, type, value, category] = line.map((cell: string) =>
        cell.trim(),
      );

      if (!title || !value || !type) return;

      categories.push(category);
      transactions.push({ title, type, value, category });
    });
    await new Promise(resolve => parseCsv.on('end', resolve));

    // verify if the categories exists in the database
    const existentCategories = await categoriesRepository.find({
      where: {
        title: In(categories),
      },
    });
    const existentCategoriesTitles = existentCategories.map(
      category => category.title,
    );

    // get categories that need to be created
    const missingCategories = categories
      .filter(category => !existentCategoriesTitles.includes(category))
      .filter((item, pos, self) => self.indexOf(item) === pos); // remove duplicates

    const newCategories = categoriesRepository.create(
      missingCategories.map(title => ({
        title,
      })),
    );
    await categoriesRepository.save(newCategories);

    const allCategories = [...newCategories, ...existentCategories];

    const createdTransactions = transactionsRepository.create(
      transactions.map(transaction => ({
        title: transaction.title,
        type: transaction.type,
        value: transaction.value,
        category: allCategories.find(
          category => category.title === transaction.category,
        ),
      })),
    );
    await transactionsRepository.save(createdTransactions);

    await fs.promises.unlink(filePath);

    return createdTransactions;
  }
}

export default ImportTransactionsService;
