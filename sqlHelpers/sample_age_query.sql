SELECT ts.insertsampleage(_sampleid := %(sampleid)s, 
                                                 _chronologyid := %(chronologyid)s, 
                                                 _age := %(age)s, 
                                                 _ageyounger := %(ageyounger)s, 
                                                 _ageolder := %(ageolder)s)