ordering = False

def check_operation(id, details):
    global ordering
    authorized = False

    print(f"[info] checking policies for event {id},"\
          f" {details['source']}->{details['deliver_to']}: {details['operation']}")
    src = details['source']
    dst = details['deliver_to']
    operation = details['operation']
    if not ordering:
        if  src == 'connector' and dst == 'mixer' \
            and operation == 'ordering' and len(details) == 9 :
            #todo check content of another fields
            #now it helps, cause if another fields set will appear - 
            #checking will be broken
            #generally, we can do it (and better case) as function to check each message between components
            check = 4
            for x in details['mix']:
                if x.find(';')>=0:
                    print('SQL injection found!')
                    check-=1
            for x in details['amount']:
                if str(x).find(';')>=0:
                    print('SQL injection found!')
                    check-=1
            for x in details['from']:
                if x.find(';')>=0:
                    print('SQL injection found!')
                    check-=1
            for x in details['using']:
                if x.find(';')>=0:
                    print('SQL injection found!')
                    check-=1
            if check==4:
                authorized = True
                ordering = True


    if src == 'mixer' and dst == 'equipment' \
        and operation == 'ask_equipment':
        authorized = True    
    if src == 'mixer' and dst == 'equipment' \
        and operation == 'equipment_status_req':
        authorized = True    
    if src == 'mixer' and dst == 'storage' \
        and operation == 'storage_book':
        authorized = True    
    if src == 'mixer' and dst == 'bre' \
        and operation == 'confirmation':
        authorized = True    
    if src == 'mixer' and dst == 'storage' \
        and operation == 'unblock':
        #and details['verified'] is True:
        authorized = True    
    if src == 'mixer' and dst == 'reporter' \
        and operation == 'operation_status':
        authorized = True    
    if src == 'mixer' and dst == 'storage' \
        and operation == 'decomission':
        authorized = True

    if src == 'storage' and dst == 'mixer' \
        and operation == 'storage_status':
        authorized = True    

    if src == 'equipment' and dst == 'mixer' \
        and operation == 'operation_status':
        authorized = True
    if src == 'equipment' and dst == 'mixer' \
        and operation == 'list_equipment':
        authorized = True
    if  src == 'equipment' and dst == 'mixer'\
        and operation == 'equipment_status':
        authorized = True

    if  src == 'document' and dst == 'reporter'\
        and operation == 'acts_req':
        authorized = True
        

    if  src == 'reporter' and dst == 'document'\
        and operation == 'need_acts':
        authorized = True
    if  src == 'reporter' and dst == 'connector'\
        and operation == 'acts':
        authorized = True
        ordering = False

    if  src == 'bre' and dst == 'mixer'\
        and operation == 'confirmation':
        authorized = True
    if  src == 'bre' and dst == 'equipment'\
        and operation == 'confirmation':
        authorized = True
    
    #authorized = True

    return authorized