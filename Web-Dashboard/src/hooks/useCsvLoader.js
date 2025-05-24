import { useState, useEffect } from 'react';
import Papa from 'papaparse';

const useCsvLoader = (path) => {
  const [data, setData] = useState([]);

  useEffect(() => {
    fetch(path)
      .then((res) => res.text())
      .then((text) => {
        Papa.parse(text, {
          header: true,
          skipEmptyLines: true,
          complete: (results) => setData(results.data),
        });
      });
  }, [path]);

  return data;
};

export default useCsvLoader;
