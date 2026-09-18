SELECT * FROM neotoma.ndb.taxa
LIMIT 10;





SELECT COUNT(*) AS occurrences
FROM t_charcoal
JOIN tr_charcoal_size
  ON t_charcoal.ID_CHARCOAL_SIZE = tr_charcoal_size.ID_CHARCOAL_SIZE;
GROUP BY CHARCOAL_SIZE_DESC
ORDER BY occurrences DESC;




INSERT INTO taxa
SELECT * FROM dblink('dbname=neotoma', 'SELECT * FROM taxa')
AS t(...column definitions...);